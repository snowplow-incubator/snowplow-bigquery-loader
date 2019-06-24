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

import io.circe.Json
import io.circe.syntax._

import com.spotify.scio.bigquery.TableRow

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field, Row}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._

import common.{Adapter, Schema}
import IdInstances._
import loader.BadRow._

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[ShreddedType])

object LoaderRow {
  final case class SelfDescribingEntityGroup(shreddedType: ShreddedType, selfDescribingJsons: NonEmptyList[SelfDescribingData[Json]])

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
      event         <- Event.parse(record).toEither.leftMap(e => ParsingError(record, e))
      rowWithTstamp <- fromEvent(singleton.ResolverSingleton.get(resolver))(event)
      (row, tstamp)  = rowWithTstamp
    } yield LoaderRow(tstamp, row, event.inventory)

  type Transformed[A] = Either[BadRow, A]

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent(resolver: Resolver[Id])(event: Event): Either[BadRow, (TableRow, Instant)] = {
    val transformed = for {
      atomic <- transformAtomicEvent(event)
      selfDescribingEntities <- transformSelfDescribingEntities(resolver, event)
    } yield atomic ++ selfDescribingEntities

    transformed.map(l => (TableRow(l: _*), new Instant(event.collector_tstamp.toEpochMilli)))
  }

  /**
    * Transforms self describing entities such as unstruct_event, context and derived_contexts in
    * event to BQ compatible format. In order to do that, it fetches schema of self describing entity
    * and creates BQ DDL schema from schema json. After, it casts data to BQ compatible format
    * with using DDL schema
    */
  def transformSelfDescribingEntities(resolver: Resolver[Id], event: Event): Transformed[List[(String, Any)]] = {
    val entityGroups = createEntityGroups(event)
    for {
      lookupRes <- lookupSchemas(resolver, entityGroups).leftMap(e => IgluError(event, e)).toEither
      ddlSchemaParseRes <- parseSchemaJsonsToDdlSchema(lookupRes).leftMap(e => RuntimeError(event, e)).toEither
      castRes <- castEntities(ddlSchemaParseRes).leftMap(e => CastError(event, e)).toEither
    } yield castRes
  }

  /**
    * Transforms atomic part of the event to key-value pairs in order to submit it to BQ
    */
  def transformAtomicEvent(event: Event): Transformed[List[(String, Any)]] =
    event
      .atomic
      .filter { case (key, value) => key == "geo_location" || !value.isNull }
      .toList
      .traverse[ValidatedNel[RuntimeErrorInfo, ?], (String, Any)] { case (key, value) =>
        value.fold(
          RuntimeErrorInfo(value, s"Unexpected JSON null in [$key]").invalidNel,   // Should not happen
          b => b.validNel,
          i => i.toInt.orElse(i.toBigDecimal).getOrElse(i.toDouble).validNel,
          s => s.validNel,
          _ => RuntimeErrorInfo(value, s"Unexpected JSON array with key [$key] found in EnrichedEvent").invalidNel,
          _ => RuntimeErrorInfo(value, s"Unexpected JSON object with key [$key] found in EnrichedEvent").invalidNel
        ).map { v => (key, v)}
      }
      .leftMap[BadRow](e => RuntimeError(event, e))
      .toEither

  /**
    * Groups self describing entities in the event such as unstruct_event, contexts
    * and derived_contexts with respect to their schema keys and creates shredded
    * type according to what type entity it is. In the end, every self describing entity
    * group will have different schema and shredded type.
    */
  def createEntityGroups(event: Event): List[SelfDescribingEntityGroup] = {
    val contextGroups = (event.contexts.data ++ event.derived_contexts.data)
      .groupBy(_.schema)
      .map {
        case (key, groupContexts) =>
          val shreddedType = ShreddedType(Contexts(CustomContexts), key)
          SelfDescribingEntityGroup(shreddedType, NonEmptyList.fromListUnsafe(groupContexts))
      }
      .toList
    val unstructEventGroup = event.unstruct_event.data.map { d =>
      val shreddedType = ShreddedType(UnstructEvent, d.schema)
      SelfDescribingEntityGroup(shreddedType, NonEmptyList.of(d))
    }
    contextGroups ++ unstructEventGroup
  }

  /**
    * Fetches schema of every self describing entity group and returns schema json
    * with the group in the end.
    */
  def lookupSchemas(
    resolver: Resolver[Id],
    entityGroups: List[SelfDescribingEntityGroup]
  ): ValidatedNel[IgluErrorInfo, List[(Json, SelfDescribingEntityGroup)]] =
    entityGroups
      .traverse[ValidatedNel[IgluErrorInfo, ?], (Json, SelfDescribingEntityGroup)] {
        entityGroup: SelfDescribingEntityGroup =>
          lookupSchema(resolver)(entityGroup.shreddedType.schemaKey)
            .map(schemaJson => (schemaJson, entityGroup))
      }

  /**
    * Parses schema jsons which are given with entity groups to Ddl schemas and
    * returns them with respective entity group
    */
  def parseSchemaJsonsToDdlSchema(
    entityGroupsWithSchema: List[(Json, SelfDescribingEntityGroup)]
  ): ValidatedNel[RuntimeErrorInfo, List[(DdlSchema, SelfDescribingEntityGroup)]] =
    entityGroupsWithSchema
      .traverse[ValidatedNel[RuntimeErrorInfo, ?], (DdlSchema, SelfDescribingEntityGroup)] {
        case (schemaJson, shreddedSelfDescribingEntity) =>
          ddlSchemaParse(schemaJson, shreddedSelfDescribingEntity.shreddedType.schemaKey)
            .map(ddlSchema => (ddlSchema, shreddedSelfDescribingEntity))
      }

  /**
    * Casts data in the entity groups into BQ compatible format with using
    * respective Ddl schemas
    */
  def castEntities(
    entityGroupsWithDdlSchema: List[(DdlSchema, SelfDescribingEntityGroup)]
  ): ValidatedNel[CastErrorInfo, List[(String, Any)]] =
    entityGroupsWithDdlSchema
      .traverse[ValidatedNel[CastErrorInfo, ?], (String, Any)] {
      case (ddlSchema, shreddedSelfDescribingEntityGroup) =>
        castData(
          shreddedSelfDescribingEntityGroup.shreddedType,
          ddlSchema,
          shreddedSelfDescribingEntityGroup.selfDescribingJsons.map(_.data)
        )
      }

  def lookupSchema(resolver: Resolver[Id])(schemaKey: SchemaKey): ValidatedNel[IgluErrorInfo, Json] =
    resolver.lookupSchema(schemaKey)
      .leftMap(IgluErrorInfo(schemaKey, _))
      .toValidatedNel

  def ddlSchemaParse(schemaJson: Json, key: SchemaKey): ValidatedNel[RuntimeErrorInfo, DdlSchema] =
    DdlSchema.parse(schemaJson).toValidNel(RuntimeErrorInfo(key.toSchemaUri.asJson, "Error while parsing ddl schema"))

  def castData(shreddedType: ShreddedType, schema: DdlSchema, jsons: NonEmptyList[Json]): ValidatedNel[CastErrorInfo, (String, Any)] = {
    val columnName = Schema.getColumnName(shreddedType)
    val field = Field.build("", schema, false)
    jsons
      .traverse[ValidatedNel[CastErrorInfo, ?], Row] { data =>
        Row.cast(field)(data).leftMap(CastErrorInfo(data, shreddedType.schemaKey, _)).toValidatedNel
      }
      .map(rows => (columnName, adaptRow(shreddedType, rows)))
  }

  def adaptRow(shreddedType: ShreddedType, rows: NonEmptyList[Row]) = shreddedType match {
    case ShreddedType(Contexts(_), _) => Adapter.adaptRow(Row.Repeated(rows.toList))
    case ShreddedType(UnstructEvent, _) => Adapter.adaptRow(rows.head)
  }
}
