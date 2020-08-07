/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import org.joda.time.Instant
import com.google.api.services.bigquery.model.TableRow

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.{IO, Timer}
import cats.implicits._
import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError, Field, Mode, Row, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import com.snowplowanalytics.snowplow.storage.bigquery.common.{Adapter, Schema}

/** Row ready to be passed into Loader stream and Mutator topic */
case class StreamLoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[ShreddedType])

object StreamLoaderRow {
  val Atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))

  val processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  /**
    * Parse the enriched TSV line into a loader row, that can be loaded into BigQuery.
    * If Loader is able to figure out that row cannot be loaded into BQ,
    * it will return a BadRow with a detailed error message.
    * If this preliminary check was passed, but the row cannot be loaded for other reasons,
    * it will be forwarded to a "failed inserts" topic, without additional information.
    * @param resolver A Resolver to be used for schema lookups.
    * @param record An enriched TSV line.
    * @return Either a BadRow with error messages or a row that is ready to be loaded.
    */
  def parse(resolver: Json)(record: String)(implicit t: Timer[IO]): IO[Either[BadRow, StreamLoaderRow]] =
    for {
      event <- IO.delay(Event.parse(record).toEither.leftMap { e =>
        BadRow.LoaderParsingError(processor, e, Payload.RawPayload(record))
      })
      igluClient <- Client.parseDefault[IO](resolver).value.flatMap(IO.fromEither)
      row <- event match {
        case Right(e) => fromEvent(igluClient)(e)
        case Left(br) => IO.delay(Left(br))
      }
    } yield row

  type Transformed[A] = ValidatedNel[FailureDetails.LoaderIgluError, A]

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent(
    igluClient: Client[IO, Json]
  )(event: Event)(implicit t: Timer[IO]): IO[Either[BadRow, StreamLoaderRow]] = {
    val atomic                                         = transformAtomic(event)
    val contexts: IO[Transformed[List[(String, Any)]]] = groupContexts(igluClient, event.contexts.data.toVector)
    val derivedContexts: IO[Transformed[List[(String, Any)]]] =
      groupContexts(igluClient, event.derived_contexts.data.toVector)
    val selfDescribingEvent: IO[Transformed[List[(String, Any)]]] = event
      .unstruct_event
      .data
      .map {
        case SelfDescribingData(schema, data) =>
          val columnName = Schema.getColumnName(ShreddedType(UnstructEvent, schema))
          transformJson(igluClient, schema)(data).map(_.map { row =>
            List((columnName, Adapter.adaptRow(row)))
          })
      }
      .getOrElse(IO.delay(List.empty[(String, Any)].validNel))

    val payload: Payload.LoaderPayload = Payload.LoaderPayload(event)

    val transformedJsons = (contexts, derivedContexts, selfDescribingEvent).mapN { (c, dc, e) =>
      (c *> dc *> e)
        .leftMap(details => Failure.LoaderIgluErrors(details))
        .leftMap(failure => BadRow.LoaderIgluError(processor, failure, payload))
        .toEither
    }

    transformedJsons.map { badRowsOrJsons =>
      for {
        jsons  <- badRowsOrJsons
        atomic <- atomic.leftMap(failure => BadRow.LoaderRuntimeError(processor, failure, payload))
        timestamp = new Instant(event.collector_tstamp.toEpochMilli)
      } yield StreamLoaderRow(
        timestamp,
        (atomic ++ jsons).foldLeft(new TableRow())((r, kv: (String, Any)) => r.set(kv._1, kv._2)),
        event.inventory
      )
    }
  }

  /** Extract all set canonical properties in BQ-compatible format */
  def transformAtomic(event: Event): Either[String, List[(String, Any)]] = {
    val aggregated =
      event.atomic.filter { case (_, value) => !value.isNull }.toList.traverse[ValidatedNel[String, ?], (String, Any)] {
        case (key, value) =>
          value
            .fold(
              s"null in $key".invalidNel,
              b => b.validNel,
              i => i.toInt.orElse(i.toBigDecimal).getOrElse(i.toDouble).validNel,
              s => s.validNel,
              _ => s"array ${value.noSpaces} in $key".invalidNel,
              _ => s"object ${value.noSpaces} in $key".invalidNel
            )
            .map { v =>
              (key, v)
            }
      }

    aggregated.leftMap(errors => s"Unexpected types in transformed event: ${errors.mkString_(",")}").toEither
  }

  /** Group list of contexts by their full URI and transform values into ready to load rows */
  def groupContexts(
    igluClient: Client[IO, Json],
    contexts: Vector[SelfDescribingData[Json]]
  )(implicit t: Timer[IO]): IO[ValidatedNel[FailureDetails.LoaderIgluError, List[(String, Any)]]] = {
    val grouped = contexts.groupBy(_.schema).map {
      case (key, groupedContexts) =>
        val contexts   = groupedContexts.map(_.data) // Strip away URI
        val columnName = Schema.getColumnName(ShreddedType(Contexts(CustomContexts), key))
        val getRow     = transformJson(igluClient, key)(_)
        contexts
          .toList
          .map(getRow)
          .sequence
          .map(_.sequence.map(rows => (columnName, Adapter.adaptRow(Row.Repeated(rows)))))
    }
    grouped.toList.sequence.map(_.sequence[ValidatedNel[FailureDetails.LoaderIgluError, ?], (String, AnyRef)])
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson(igluClient: Client[IO, Json], schemaKey: SchemaKey)(
    data: Json
  )(implicit t: Timer[IO]): IO[ValidatedNel[FailureDetails.LoaderIgluError, Row]] =
    igluClient
      .resolver
      .lookupSchema(schemaKey)
      .map(
        _.leftMap { e =>
          NonEmptyList.one(FailureDetails.LoaderIgluError.IgluError(schemaKey, e))
        }.flatMap(schema => DdlSchema.parse(schema).toRight(invalidSchema(schemaKey)))
          .map(schema => Field.build("", schema, false))
          .flatMap(field => Row.cast(field)(data).leftMap(e => e.map(castError(schemaKey))).toEither)
          .toValidated
      )

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
