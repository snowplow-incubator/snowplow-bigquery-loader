/*
 * Copyright (c) 2018-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SelfDescribingData, SchemaKey}
import com.snowplowanalytics.iglu.schemaddl.bigquery._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Failure, Payload, FailureDetails}
import cats.Monad
import cats.data.{EitherT, Validated, ValidatedNel, NonEmptyList}
import cats.effect.Clock
import cats.implicits._
import com.google.api.services.bigquery.model.TableRow
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverResult
import com.snowplowanalytics.iglu.client.resolver.StorageTime
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import io.circe.{Encoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.joda.time.Instant

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[ShreddedType])

object LoaderRow {

  type Transformed = ValidatedNel[FailureDetails.LoaderIgluError, List[(String, AnyRef)]]

  val LoadTstampField = Field("load_tstamp", Type.Timestamp, Mode.Nullable)

  val MaxBadRowPayloadLength = 5000000

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
  def parse[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F], processor: Processor, fieldCache: FieldCache[F])(
    record: String
  ): F[Either[BadRow, LoaderRow]] =
    Event.parse(record) match {
      case Validated.Valid(event) =>
        fromEvent[F](resolver, processor, fieldCache)(event)
      case Validated.Invalid(error) =>
        val badRowError = BadRow.LoaderParsingError(processor, error, Payload.RawPayload(record.take(MaxBadRowPayloadLength)))
        Monad[F].pure(badRowError.asLeft)
    }

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    processor: Processor,
    fieldCache: FieldCache[F]
  )(event: Event): F[Either[BadRow, LoaderRow]] = {
    val atomic                   = transformAtomic(event)
    val contexts: F[Transformed] = groupContexts[F](resolver, fieldCache, event.contexts.data.toVector)
    val derivedContexts: F[Transformed] =
      groupContexts(resolver, fieldCache, event.derived_contexts.data.toVector)

    val selfDescribingEvent: F[Transformed] = event
      .unstruct_event
      .data
      .map {
        case SelfDescribingData(schema, data) =>
          val columnName = Schema.getColumnName(ShreddedType(UnstructEvent, schema))
          transformJson[F](resolver, fieldCache, schema)(data).map(_.map { row =>
            List((columnName, Adapter.adaptRow(row)))
          })
      }
      .getOrElse(Monad[F].pure(List.empty[(String, AnyRef)].validNel[FailureDetails.LoaderIgluError]))

    val payload: Payload.LoaderPayload = Payload.LoaderPayload(event)

    val transformedJsons = (contexts, derivedContexts, selfDescribingEvent).mapN { (c, dc, e) =>
      (c, dc, e)
        .mapN { (c, dc, e) =>
          c ++ dc ++ e
        }
        .leftMap(details => Failure.LoaderIgluErrors(details))
        .leftMap(failure => BadRow.LoaderIgluError(processor, failure, payload))
        .toEither
    }

    transformedJsons.map { badRowsOrJsons =>
      for {
        jsons  <- badRowsOrJsons
        atomic <- atomic.leftMap(failure => BadRow.LoaderRuntimeError(processor, failure, payload))
        timestamp = new Instant(event.collector_tstamp.toEpochMilli)
      } yield LoaderRow(
        timestamp,
        (atomic ++ jsons).foldLeft(new TableRow())((r, kv) => r.set(kv._1, kv._2)),
        event.inventory
      )
    }
  }

  /** Extract all set canonical properties in BQ-compatible format */
  def transformAtomic(event: Event): Either[String, List[(String, Any)]] = {
    val aggregated =
      event.atomic.filter { case (_, value) => !value.isNull }.toList.traverse[ValidatedNel[String, *], (String, Any)] {
        case (key, value) =>
          value
            .fold(
              s"null in $key".invalidNel,
              b => b.validNel,
              i => i.toInt.orElse[Any](i.toBigDecimal).getOrElse(i.toDouble).validNel,
              s => s.validNel,
              _ => s"array ${value.noSpaces} in $key".invalidNel,
              _ => s"object ${value.noSpaces} in $key".invalidNel
            )
            .map { v =>
              (key, v)
            }
      }

    aggregated
      .map((LoadTstampField.name, "AUTO") :: _)
      .leftMap(errors => s"Unexpected types in transformed event: ${errors.mkString_(",")}")
      .toEither
  }

  /** Group list of contexts by their full URI and transform values into ready to load rows */
  def groupContexts[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    fieldCache: FieldCache[F],
    contexts: Vector[SelfDescribingData[Json]]
  ): F[ValidatedNel[FailureDetails.LoaderIgluError, List[(String, AnyRef)]]] = {
    val grouped = contexts.groupBy(_.schema).map {
      case (key, groupedContexts) =>
        val contexts: Seq[Json] = groupedContexts.map(_.data) // Strip away URI
        val columnName          = Schema.getColumnName(ShreddedType(Contexts(CustomContexts), key))
        val getRow              = transformJson[F](resolver, fieldCache, key)(_)

        contexts
          .toList
          .map(getRow)
          .sequence
          .map(_.sequence.map(rows => (columnName, Adapter.adaptRow(Row.Repeated(rows)))))
    }
    grouped.toList.sequence.map(_.sequence[ValidatedNel[FailureDetails.LoaderIgluError, *], (String, AnyRef)])
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    fieldCache: FieldCache[F],
    schemaKey: SchemaKey
  )(
    data: Json
  ): F[Validated[NonEmptyList[LoaderIgluError], Row]] =
    getSchemaAsField(resolver, schemaKey, fieldCache)
      .value
      .map(_.flatMap(field => Row.cast(field)(data).leftMap(e => e.map(castError(schemaKey))).toEither).toValidated)

  private def schemaToField(schema: Json, schemaKey: SchemaKey): Either[NonEmptyList[LoaderIgluError], Field] =
    DdlSchema.parse(schema).toRight(invalidSchema(schemaKey)).map(schema => Field.build("", schema, false))

  private def lookupInFieldCache[F[_]: Monad](
    fieldCache: FieldCache[F],
    resolvedSchema: ResolverResult.Cached[SchemaKey, Json]
  ): EitherT[F, NonEmptyList[LoaderIgluError], Field] = {
    val fieldKey: (SchemaKey, StorageTime) = (resolvedSchema.key, resolvedSchema.timestamp)

    EitherT.liftF(fieldCache.get(fieldKey)).flatMap {
      case Some(field) => EitherT.pure[F, NonEmptyList[LoaderIgluError]](field)
      case None => {
        val field = schemaToField(resolvedSchema.value, resolvedSchema.key)
        field.toEitherT[F].semiflatTap(field => fieldCache.put(fieldKey, field))
      }
    }
  }

  private[common] def getSchemaAsField[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F],
    schemaKey: SchemaKey,
    fieldCache: FieldCache[F]
  ): EitherT[F, NonEmptyList[LoaderIgluError], Field] =
    EitherT(resolver.lookupSchemaResult(schemaKey))
      .leftMap(resolutionError => NonEmptyList.one(FailureDetails.LoaderIgluError.IgluError(schemaKey = schemaKey, resolutionError)))
      .flatMap {
        case cached: ResolverResult.Cached[SchemaKey, Json] =>
          lookupInFieldCache(fieldCache, cached)
        case ResolverResult.NotCached(result) => EitherT(
          Monad[F].pure(schemaToField(result, schemaKey))
        )
      }

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

  implicit val bqTypeEncoder: Encoder[Type] = deriveEncoder[Type]

  implicit val bqFieldEncoder: Encoder[Field] = deriveEncoder[Field]

  private def invalidSchema(schemaKey: SchemaKey): NonEmptyList[FailureDetails.LoaderIgluError] = {
    val error = FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")
    NonEmptyList.one(error)
  }
}
