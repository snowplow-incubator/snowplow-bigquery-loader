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

import cats.Id
import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import io.circe.{ Json, Encoder }
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.spotify.scio.bigquery.TableRow

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData, SchemaVer}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError, Field, Mode, Row, Type}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._

import com.snowplowanalytics.snowplow.badrows.{ Payload, Processor, Failure, FailureDetails, BadRow }

import common.{Adapter, Schema}
import IdInstances._

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[ShreddedType])

object LoaderRow {

  val Atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0))

  val processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

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
  def parse(resolver: Json)(record: String): Either[BadRow, LoaderRow] =
    for {
      event         <- Event.parse(record).toEither.leftMap { e =>
        BadRow.LoaderParsingError(processor, e, Payload.RawPayload(record))
      }
      rowWithTstamp <- fromEvent(singleton.ResolverSingleton.get(resolver))(event)
      (row, tstamp)  = rowWithTstamp
    } yield LoaderRow(tstamp, row, event.inventory)

  type Transformed[A] = ValidatedNel[FailureDetails.LoaderIgluError, A]

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent(resolver: Resolver[Id])(event: Event): Either[BadRow, (TableRow, Instant)] = {
    val atomic = transformAtomic(event)
    val contexts: Transformed[List[(String, Any)]] = groupContexts(resolver, event.contexts.data.toVector)
    val derivedContexts: Transformed[List[(String, Any)]] = groupContexts(resolver, event.derived_contexts.data.toVector)
    val selfDescribingEvent: Transformed[List[(String, Any)]] = event.unstruct_event.data.map {
      case SelfDescribingData(schema, data) =>
        val columnName = Schema.getColumnName(ShreddedType(UnstructEvent, schema))
        transformJson(resolver, schema)(data).map { row => List((columnName, Adapter.adaptRow(row))) }
    }.getOrElse(List.empty[(String, Any)].validNel)

    val payload: Payload.LoaderPayload = Payload.LoaderPayload(event)

    val transformedJsons = (contexts, derivedContexts, selfDescribingEvent)
      .mapN { (c, dc, e) => c ++ dc ++ e }
      .leftMap(details => Failure.LoaderIgluErrors(details))
      .leftMap(failure => BadRow.LoaderIgluError(processor, failure, payload))
      .toEither

    for {
      jsons <- transformedJsons
      atomic <- atomic.leftMap(failure => BadRow.LoaderRuntimeError(processor, failure, payload))
      timestamp = new Instant(event.collector_tstamp.toEpochMilli)
    } yield (TableRow(atomic ++ jsons: _*), timestamp)
  }

  /** Extract all set canonical properties in BQ-compatible format */
  def transformAtomic(event: Event): Either[String, List[(String, Any)]] = {
    val aggregated = event
      .atomic
      .filter { case (_, value) => !value.isNull }
      .toList
      .traverse[ValidatedNel[String, ?], (String, Any)] { case (key, value) =>
        value.fold(
          s"null in $key".invalidNel,
          b => b.validNel,
          i => i.toInt.orElse(i.toBigDecimal).getOrElse(i.toDouble).validNel,
          s => s.validNel,
          _ => s"array ${value.noSpaces} in $key".invalidNel,
          _ => s"object ${value.noSpaces} in $key".invalidNel
        ).map { v => (key, v) }
      }

    aggregated
      .leftMap(errors => s"Unexpected types in transformed event: ${errors.mkString_(",")}")
      .toEither
  }

  /** Group list of contexts by their full URI and transform values into ready to load rows */
  def groupContexts(resolver: Resolver[Id], contexts: Vector[SelfDescribingData[Json]]): ValidatedNel[FailureDetails.LoaderIgluError, List[(String, Any)]] = {
    val grouped = contexts.groupBy(_.schema).map { case (key, groupedContexts) =>
      val contexts = groupedContexts.map(_.data)    // Strip away URI
      val columnName = Schema.getColumnName(ShreddedType(Contexts(CustomContexts), key))
      val getRow = transformJson(resolver, key)(_)
      contexts
        .toList
        .traverse[ValidatedNel[FailureDetails.LoaderIgluError, ?], Row](getRow)
        .map(rows => (columnName, Adapter.adaptRow(Row.Repeated(rows))))
    }
    grouped
      .toList
      .sequence[ValidatedNel[FailureDetails.LoaderIgluError, ?], (String, AnyRef)]
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson(resolver: Resolver[Id], schemaKey: SchemaKey)
                   (data: Json): ValidatedNel[FailureDetails.LoaderIgluError, Row] =
    resolver.lookupSchema(schemaKey)
      .leftMap { e => NonEmptyList.one(FailureDetails.LoaderIgluError.IgluError(schemaKey, e)) }
      .flatMap(schema => DdlSchema.parse(schema).toRight(invalidSchema(schemaKey)))
      .map(schema => Field.build("", schema, false))
      .flatMap(field => Row.cast(field)(data).leftMap(e => e.map(castError(schemaKey))).toEither)
      .toValidated


  def castError(schemaKey: SchemaKey)(error: CastError): FailureDetails.LoaderIgluError =
    error match {
      case CastError.MissingInValue(key, value) =>
        FailureDetails.LoaderIgluError.MissingInValue(schemaKey, key, value)
      case CastError.NotAnArray(value, expected) =>
        FailureDetails.LoaderIgluError.NotAnArray(schemaKey, value, expected.asJson.noSpaces)
      case CastError.WrongType(value, expected) =>
        FailureDetails.LoaderIgluError.WrongType(schemaKey, value, expected.asJson.noSpaces)
    }

  implicit val bqModeEncoder: Encoder[Mode] = deriveEncoder[Mode]

  implicit def bqTypeEncoder: Encoder[Type] = deriveEncoder[Type]

  implicit def bqFieldEncoder: Encoder[Field] = deriveEncoder[Field]

  private def invalidSchema(schemaKey: SchemaKey): NonEmptyList[FailureDetails.LoaderIgluError] = {
    val error = FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")
    NonEmptyList.one(error)
  }

}
