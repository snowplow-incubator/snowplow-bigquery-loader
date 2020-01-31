/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import org.joda.time.Instant

import cats.data.{EitherNel, NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

import io.circe.Json

import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, fromJsonNode}

import com.spotify.scio.bigquery.TableRow

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError, Field, Row}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data._
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer.getValidatedJsonEvent

import common.{Adapter, Schema, Utils => CommonUtils}

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[InventoryItem])

object LoaderRow {

  /**
    * Parse enriched TSV into a loader row, ready to be loaded into bigquery
    * If Loader is able to figure out that row cannot be loaded into BQ -
    * it will return return BadRow with detailed error that later can be analyzed
    * If premature check has passed, but row cannot be loaded it will be forwarded
    * to "failed inserts" topic, without additional information
    * @param resolver serializable Resolver's config to get it from singleton store
    * @param record enriched TSV line
    * @return either bad with error messages or entity ready to be loaded
    */
  def parse(resolver: JValue)(record: String): Either[BadRow, LoaderRow] = {
    val loaderRow: EitherNel[String, LoaderRow] = for {
      (inventory, event) <- getValidatedJsonEvent(record.split("\t", -1), false).leftMap(e =>
        NonEmptyList.fromListUnsafe(e)
      ): EitherNel[String, (Set[InventoryItem], JObject)]
      rowWithTstamp <- fromJson(singleton.ResolverSingleton.get(resolver))(event)
      (row, tstamp) = rowWithTstamp
    } yield LoaderRow(tstamp, row, inventory)

    loaderRow.leftMap(errors => BadRow(record, errors))
  }

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromJson(resolver: Resolver)(json: JObject): EitherNel[String, (TableRow, Instant)] = {
    val validated = json.obj.collectFirst { case ("collector_tstamp", JString(tstamp)) => tstamp } match {
      case Some(tstamp) =>
        val atomicFields: List[ValidatedNel[String, List[(String, Any)]]] = json.obj.map {
          case ("geo_location", _)      => Nil.validNel
          case (_, JNull)               => Nil.validNel
          case (_, JNothing)            => Nil.validNel
          case (key, JString(str))      => List((key, str)).validNel
          case (key, JInt(int))         => List((key, int)).validNel
          case (key, JDouble(double))   => List((key, double)).validNel
          case (key, JDecimal(decimal)) => List((key, decimal)).validNel
          case (key, JBool(bool))       => List((key, bool)).validNel
          case (key, o: JObject) if key == "unstruct_event" =>
            parseSelfDescribingJson(resolver)(o)
          case (key, o: JObject) if key == "contexts" || key == "derived_contexts" =>
            parseSelfDescribingJson(resolver)(o)
          case (key, _: JObject) => s"Unexpected JSON object with key [$key] found in EnrichedEvent".invalidNel
          case (key, _: JArray)  => s"Unexpected JSON array with key [$key] found in EnrichedEvent".invalidNel
        }

        val fields: ValidatedNel[String, List[(String, Any)]] =
          atomicFields.sequence[ValidatedNel[String, ?], List[(String, Any)]].map(_.flatten)
        val time: ValidatedNel[String, Instant] =
          Validated
            .catchNonFatal(Instant.parse(tstamp))
            .leftMap(err => NonEmptyList.one(s"Cannot extract collect_tstamp: ${err.getMessage}"))

        fields.product(time).map { case (f, t) => (TableRow(f: _*), t) }
      case None =>
        "No collector_tstamp".invalidNel
    }

    validated.toEither
  }

  /**
    * Turn enriched event's `contexts` or `derived_contexts` content into list of key-value pairs,
    * where key is a column name and value is ready to load array of rows
    */
  def parseSelfDescribingJson(resolver: Resolver)(payload: JObject): ValidatedNel[String, List[(String, Any)]] =
    CommonUtils
      .toCirce(payload)
      .toData
      .toList
      .traverse[ValidatedNel[String, ?], List[(String, Any)]] {
        case SelfDescribingData(
            SchemaKey("com.snowplowanalytics.snowplow", "contexts", _, SchemaVer.Full(1, _, _)),
            payload
            ) =>
          val contexts = payload.asArray.getOrElse(Vector.empty).flatMap(_.toData.toVector)
          groupContexts(resolver, contexts)
        case SelfDescribingData(
            SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", _, SchemaVer.Full(1, _, _)),
            uePayload
            ) =>
          uePayload.toData match {
            case Some(SelfDescribingData(key, json)) =>
              val columnName = Schema.getColumnName(InventoryItem(UnstructEvent, key.toSchemaUri))
              transformJson(resolver)(key)(json).map { row =>
                List((columnName, Adapter.adaptRow(row)))
              }
            case None => "Cannot decode unstruct_event as self-describing JSON".invalidNel[List[(String, Any)]]
          }
        case _ =>
          s"Cannot recognize JSON payload as any known self-describing type ${compact(payload)}"
            .invalidNel[List[(String, Any)]]
      }
      .map(_.flatten)

  /** Group list of contexts by their full URI and transform values into ready to load rows */
  def groupContexts(
    resolver: Resolver,
    contexts: Vector[SelfDescribingData[Json]]
  ): ValidatedNel[String, List[(String, Any)]] = {
    val grouped = contexts.groupBy(_.schema).map {
      case (key, groupedContexts) =>
        val contexts   = groupedContexts.map(_.data) // Strip away URI
        val columnName = Schema.getColumnName(InventoryItem(Contexts(CustomContexts), key.toSchemaUri))
        val getRow     = transformJson(resolver)(key)(_)
        contexts
          .toList
          .traverse[ValidatedNel[String, ?], Row](getRow)
          .map(rows => (columnName, Adapter.adaptRow(Row.Repeated(rows))))
    }
    grouped.toList.sequence[ValidatedNel[String, ?], (String, AnyRef)]
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson(resolver: Resolver)(schemaKey: SchemaKey)(data: Json): ValidatedNel[String, Row] = {
    def stringifyNel[E](nel: NonEmptyList[E]): NonEmptyList[String] =
      nel.map(_.toString)

    CommonUtils
      .fromValidationZ(resolver.lookupSchema(schemaKey.toSchemaUri).map(fromJsonNode))
      .leftMap(stringifyNel)
      .andThen(schema => DdlSchema.parse(schema).toValidNel(s"Cannot parse JSON Schema [$schemaKey]"))
      .map(schema => Field.build("", schema, false))
      .andThen(Row.cast(_)(data).leftMap(stringifyCastErrors))
  }

  def stringifyCastErrors(nel: NonEmptyList[CastError]): NonEmptyList[String] =
    nel.map {
      case CastError.WrongType(value, expected) =>
        Json
          .fromFields(
            List(
              ("message", Json.fromString("Unexpected type of value")),
              ("value", value),
              ("expectedType", Json.fromString(expected.toString))
            )
          )
          .noSpaces
      case CastError.NotAnArray(value, expected) =>
        Json
          .fromFields(
            List(
              ("message", Json.fromString("Value should be in array")),
              ("value", value),
              ("expectedType", Json.fromString(expected.toString))
            )
          )
          .noSpaces
      case CastError.MissingInValue(key, value) =>
        Json
          .fromFields(
            List(
              ("message", Json.fromString("Key is missing in value")),
              ("value", value),
              ("missingKey", Json.fromString(key))
            )
          )
          .noSpaces
    }
}
