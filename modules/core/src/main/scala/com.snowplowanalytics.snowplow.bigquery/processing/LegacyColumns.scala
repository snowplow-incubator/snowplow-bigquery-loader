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
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field => LegacyField, Mode => LegacyMode, Type => LegacyType}
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, Caster, Field => V2Field, Type => V2Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data => SdkData, Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.loaders.transform.{SchemaSubVersion, TabledEntity}
import com.snowplowanalytics.snowplow.badrows.{
  BadRow,
  Failure => BadRowFailure,
  FailureDetails,
  Payload => BadPayload,
  Processor => BadRowProcessor
}

/**
 * The methods needed to support the legacy column style of v1 loader
 *
 * These methods are largely copied from the `loaders-common` module of common-streams, but adapted
 * for the bigquery types in schema-ddl
 *
 * If we ever end support for legacy columns, then everything in this file can be deleted.
 */
object LegacyColumns {

  /**
   * The analog of NonAtomicFields.Result from common-streams, but adapted for the legacy bigquery
   * types in schema-ddl
   *
   * @param fields
   *   field type information about each self-describing entity to be loaded
   * @param igluFailures
   *   details of schemas that were present in the batch but could not be looked up by the Iglu
   *   resolver.
   */
  case class Result(fields: Vector[FieldForEntity], igluFailures: List[ColumnFailure])

  /**
   * Field type information for a self-describing entity to be loaded
   *
   * @param field
   *   The schema-ddl Field describing the entity to be loaded. Note this is a v2 Field from the
   *   parquet package of schema-ddl, so it must be converted from the legacy v1 Field.
   * @param key
   *   The Iglu schema key for this entity
   * @param entityType
   *   Whether this is for an unstruct_event or a context
   */
  case class FieldForEntity(
    field: V2Field,
    key: SchemaKey,
    entityType: TabledEntity.EntityType
  )

  /**
   * Describes a failure to lookup an Iglu schema
   *
   * @param schemaKey
   *   The Iglu schema key of the failure
   * @param entityType
   *   Whether the lookup was done for an unstruct_event or a context
   * @param failure
   *   Why the lookup failed
   */
  case class ColumnFailure(
    schemaKey: SchemaKey,
    entityType: TabledEntity.EntityType,
    failure: FailureDetails.LoaderIgluError
  )

  /**
   * The analog of `NonAtomicFields.resolveTypes` from common-streams, but adapted for the legacy
   * bigquery types in schema-ddl
   *
   * @param resolver
   *   The Iglu resolver
   * @param entities
   *   The self-describing entities in this batch for which we need to load data
   * @param matchCriteria
   *   Lists the schemas that we want to load in the legacy v1 column style
   * @return
   *   The typed fields for the entities to be loaded. These are v2 fields (parquet fields).
   *
   * This method works by deriving the legacy schema-ddl fields and then converting to the
   * equivalent v2 (parquet) schema-ddl field.
   */
  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    matchCriteria: List[SchemaCriterion]
  ): F[Result] =
    entities.toVector
      .flatMap { case (tabledEntity, subVersions) =>
        subVersions
          .map { subVersion =>
            (tabledEntity.entityType, TabledEntity.toSchemaKey(tabledEntity, subVersion))
          }
          .filter { case (_, schemaKey) =>
            matchCriteria.exists(_.matches(schemaKey))
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
            val legacyField = LegacyField.build(columnName, schema, false).setMode(mode)
            val v2Field     = V2Field.normalize(legacyFieldToV2Field(legacyField)).copy(accessors = Set.empty)
            FieldForEntity(v2Field, schemaKey, entityType)
          }
          .leftMap(ColumnFailure(schemaKey, entityType, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        Result(good, failures.toList)
      }

  /**
   * The analog of `Transform.transformEvent` from common-streams
   *
   * @param processor
   *   Details about this loader, only used for generating a bad row
   * @param event
   *   The Snowplow event received from the stream
   * @param batchInfo
   *   Pre-calculated Iglu types for a batch of events, used to guide the transformation
   * @return
   *   If event entities are valid, then returns a Map that is compatible with the BigQuery Write
   *   client. If event entities are invalid then returns a bad row.
   */
  def transformEvent(
    processor: BadRowProcessor,
    event: Event,
    batchInfo: Result
  ): Either[BadRow, Map[String, AnyRef]] =
    failForResolverErrors(processor, event, batchInfo.igluFailures) *>
      forEntities(event, batchInfo.fields).leftMap { nel =>
        BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event))
      }.toEither

  /**
   * Replaces any Json type with a String type
   *
   * This must be run after transformation, but before handling schema evolution. It is needed
   * because the legacy loader used String columns where the v2 loader would create Json columns
   */
  def dropJsonTypes(v2Field: V2Field): V2Field = {
    def dropFromType(t: V2Type): V2Type = t match {
      case V2Type.Json                        => V2Type.String
      case V2Type.Array(element, nullability) => V2Type.Array(dropFromType(element), nullability)
      case V2Type.Struct(fields)              => V2Type.Struct(fields.map(dropJsonTypes))
      case other                              => other
    }

    v2Field.copy(fieldType = dropFromType(v2Field.fieldType))

  }

  /**
   * Convert from the legacy Field of the old v1 loader into the new style Field.
   *
   * This is needed because this loader only deals with v2 Fields. But we want to use the legacy
   * methods from schema-ddl which only return legacy Fields.
   */
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
      case LegacyType.String    => V2Type.Json
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

  private object LegacyCaster extends BigQueryCaster {
    override def jsonValue(v: Json): String =
      v.asString match {
        case Some(s) =>
          // This line differs from the V2 behaviour. If the original value was a plain string then
          // we load the plain string without quotes. Whereas in v2 we would load it as a quoted
          // JSON string.
          s
        case None =>
          v.noSpaces
      }
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
      case CastError.WrongType(v, e) =>
        val fixedType = e match {
          case V2Type.Json => V2Type.String.toString
          case other       => other.toString
        }
        FailureDetails.LoaderIgluError.WrongType(schemaKey, v, fixedType)
      case CastError.MissingInValue(k, v) =>
        FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def forUnstruct(
    field: V2Field,
    schemaKey: SchemaKey,
    event: Event
  ): ValidatedNel[CastError, AnyRef] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(key2, unstructData)) if schemaKey === key2 =>
        Caster.cast(LegacyCaster, field, unstructData)
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
      Caster.cast(LegacyCaster, field, Json.fromValues(matchingContexts.map(_.data)))
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
