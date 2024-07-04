/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Eq
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import cats.effect.Sync
import io.circe.Json

import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field => LegacyField, Mode => LegacyMode, Type => LegacyType}
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, Caster, Field => V2Field, Type => V2Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data => SdkData, Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity, Transform}
import com.snowplowanalytics.snowplow.badrows.{
  BadRow,
  Failure => BadRowFailure,
  FailureDetails,
  Payload => BadPayload,
  Processor => BadRowProcessor
}

object LegacyColumns {

  case class Result(fields: Vector[FieldForEntity], igluFailures: List[ColumnFailure])

  case class FieldForEntity(
    field: V2Field,
    key: SchemaKey,
    entityType: TabledEntity.EntityType
  )

  case class ColumnFailure(
    schemaKey: SchemaKey,
    entityType: TabledEntity.EntityType,
    failure: FailureDetails.LoaderIgluError
  )

  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]]
  ): F[Result] =
    entities.toVector
      .flatMap { case (tabledEntity, subVersions) =>
        subVersions.map { subVersion =>
          (tabledEntity.entityType, TabledEntity.toSchemaKey(tabledEntity, subVersion))
        }
      }
      .traverse { case (entityType, schemaKey) =>
        getSchema(resolver, schemaKey)
          .map { schema =>
            val mode = entityType match {
              case TabledEntity.UnstructEvent => LegacyMode.Nullable
              case TabledEntity.Context       => LegacyMode.Repeated
            }
            val columnName  = legacyColumnName(entityType, schemaKey)
            val legacyField = LegacyField.build(columnName, schema, false).setMode(mode).normalized
            val v2Field     = legacyFieldToV2Field(legacyField)
            FieldForEntity(v2Field, schemaKey, entityType)
          }
          .leftMap(ColumnFailure(schemaKey, entityType, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        Result(good, failures.toList)
      }

  def transformEvent(
    processor: BadRowProcessor,
    event: Event,
    batchInfo: Result,
    loadTstamp: java.lang.Long
  ): Either[BadRow, Map[String, AnyRef]] =
    for {
      _ <- failForResolverErrors(processor, event, batchInfo.igluFailures)
      atomic <- forAtomic(processor, event)
      nonAtomic <- forEntities(event, batchInfo.fields).leftMap { nel =>
                     BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event))
                   }.toEither
    } yield atomic ++ nonAtomic + ("load_tstamp" -> loadTstamp)

  private def legacyFieldToV2Field(legacyField: LegacyField): V2Field = {
    val fieldType = legacyTypeToV2Type(legacyField.fieldType)
    legacyField.mode match {
      case LegacyMode.Nullable =>
        V2Field(legacyField.name, fieldType, V2Type.Nullability.Nullable)
      case LegacyMode.Required =>
        V2Field(legacyField.name, fieldType, V2Type.Nullability.Required)
      case LegacyMode.Repeated =>
        val repeatedType = V2Type.Array(fieldType, V2Type.Nullability.Nullable)
        V2Field(legacyField.name, repeatedType, V2Type.Nullability.Nullable)
    }
  }

  private def legacyTypeToV2Type(legacyType: LegacyType): V2Type =
    legacyType match {
      case LegacyType.String    => V2Type.String
      case LegacyType.Boolean   => V2Type.Boolean
      case LegacyType.Integer   => V2Type.Long
      case LegacyType.Float     => V2Type.Double
      case LegacyType.Numeric   => V2Type.Decimal(V2Type.DecimalPrecision.Digits38, 9)
      case LegacyType.Date      => V2Type.Date
      case LegacyType.DateTime  => V2Type.Timestamp
      case LegacyType.Timestamp => V2Type.Timestamp
      case LegacyType.Record(nested) =>
        nested.toNel match {
          case None      => V2Type.Json
          case Some(nel) => V2Type.Struct(nel.toNev.map(legacyFieldToV2Field))
        }
    }

  private def getSchema[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, Schema] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey))
                .leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield schema

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

  private implicit val eqSchemaKey: Eq[SchemaKey] = Eq.fromUniversalEquals

  private def failForResolverErrors(
    processor: BadRowProcessor,
    event: Event,
    failures: List[ColumnFailure]
  ): Either[BadRow, Unit] = {
    val schemaFailures = failures.flatMap { case ColumnFailure(failureSchemaKey, entityType, failure) =>
      entityType match {
        case TabledEntity.UnstructEvent =>
          event.unstruct_event.data match {
            case Some(SelfDescribingData(schemaKey, _)) if schemaKey === failureSchemaKey =>
              Some(failure)
            case _ =>
              None
          }
        case TabledEntity.Context =>
          val allContexts = event.contexts.data.iterator ++ event.derived_contexts.data.iterator
          if (allContexts.exists(context => context.schema === failureSchemaKey))
            Some(failure)
          else
            None
      }
    }

    NonEmptyList.fromList(schemaFailures) match {
      case None => Right(())
      case Some(nel) =>
        Left(BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event)))
    }
  }

  private def forAtomic(processor: BadRowProcessor, event: Event): Either[BadRow, Map[String, AnyRef]] =
    Transform
      .transformEvent[AnyRef](processor, BigQueryCaster, event, NonAtomicFields.Result(Vector.empty, List.empty))
      .map { namedValues =>
        namedValues.map { case Caster.NamedValue(k, v) =>
          k -> v
        }.toMap
      }

  private def forEntities(
    event: Event,
    entities: Vector[FieldForEntity]
  ): ValidatedNel[FailureDetails.LoaderIgluError, Map[String, AnyRef]] =
    entities
      .traverse { case FieldForEntity(field, schemaKey, entityType) =>
        val result = entityType match {
          case TabledEntity.UnstructEvent => forUnstruct(field, schemaKey, event)
          case TabledEntity.Context       => forContexts(field, schemaKey, event)
        }
        result
          .map { fieldValue =>
            field.name -> fieldValue
          }
          .leftMap { failure =>
            castErrorToLoaderIgluError(schemaKey, failure)
          }
      }
      .map(_.toMap)

  private def castErrorToLoaderIgluError(
    schemaKey: SchemaKey,
    castErrors: NonEmptyList[CastError]
  ): NonEmptyList[FailureDetails.LoaderIgluError] =
    castErrors.map {
      case CastError.WrongType(v, e)      => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def forUnstruct(
    field: V2Field,
    schemaKey: SchemaKey,
    event: Event
  ): ValidatedNel[CastError, AnyRef] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(key2, unstructData)) if schemaKey === key2 =>
        Caster.cast(BigQueryCaster, field, unstructData)
      case _ =>
        Validated.Valid(null)
    }

  private def forContexts[A](
    field: V2Field,
    schemaKey: SchemaKey,
    event: Event
  ): ValidatedNel[CastError, AnyRef] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => context.schema === schemaKey)

    if (matchingContexts.nonEmpty)
      Caster.cast(BigQueryCaster, field, Json.fromValues(matchingContexts.map(_.data)))
    else
      Validated.Valid(null)
  }

  private def legacyColumnName(entityType: TabledEntity.EntityType, schemaKey: SchemaKey): String = {
    val SchemaVer.Full(model, revision, addition) = schemaKey.version
    val shredProperty = entityType match {
      case TabledEntity.UnstructEvent => SdkData.UnstructEvent
      case TabledEntity.Context       => SdkData.Contexts(SdkData.CustomContexts)
    }
    val v2Name = SnowplowEvent.transformSchema(shredProperty, schemaKey.vendor, schemaKey.name, model)
    s"${v2Name}_${revision}_${addition}"
  }

}
