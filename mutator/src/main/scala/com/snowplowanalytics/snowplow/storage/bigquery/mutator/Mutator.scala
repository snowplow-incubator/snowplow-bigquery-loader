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
package mutator

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.MVar
import org.json4s.jackson.JsonMethods.fromJsonNode
import com.google.cloud.bigquery.Field
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Generator, Field => BigQueryField, Mode}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.{IgluUri, InventoryItem}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data
import Mutator._
import cats.Monad
import common.{Adapter, Utils, Schema => LoaderSchema}
import common.Config._

/**
  * Mutator is stateful worker that emits `alter table` requests.
  * It does not depend on any source of requests (such as PubSub topic)
  *
  * @param resolver iglu resolver, responsible for fetching schemas
  * @param tableReference object responsible for table interactions
  * @param state current state of the table, source of truth
  */
class Mutator private(resolver: Resolver,
                      tableReference: TableReference,
                      state: MVar[IO, Vector[Field]]) {

  /** Add all new columns to a table */
  def updateTable(inventoryItems: List[InventoryItem]): IO[Unit] =
    for {
      fields <- state.read
      // flattening means that we're simply ignoring fields without iglu URI in description
      existingTypes = fields.map(_.getDescription).flatMap(SchemaKey.fromUri)
      fieldsToAdd = filterFields(existingTypes, inventoryItems)
      _ <- fieldsToAdd.traverse_(addField)
    } yield ()

  /** Perform ALTER TABLE */
  def addField(inventoryItem: InventoryItem): IO[Unit] =
    for {
      schema <- getSchema(inventoryItem.igluUri)
      newField = getField(inventoryItem, schema)
      _ <- tableReference.addField(newField)
      stateFields <- state.take
      _ <- state.put(stateFields :+ newField)
      _ <- IO(println(s"Added ${newField.getName}"))
    } yield ()

  /** Transform Iglu Schema into valid BigQuery field */
  def getField(inventoryItem: InventoryItem, schema: Schema): Field = {
    val mode = inventoryItem.shredProperty match {
      case Data.UnstructEvent => Mode.Nullable
      case Data.Contexts(_) => Mode.Repeated
    }

    val columnName = LoaderSchema.getColumnName(inventoryItem)
    val field = Generator.build(columnName, schema, false).setMode(mode).normalized
    val column = Generator.Column(columnName, field, SchemaKey.fromUri(inventoryItem.igluUri).get)

    Adapter.fromColumn(column)
  }

  /** Receive the JSON Schema from the Iglu registry */
  def getSchema(igluUri: IgluUri): IO[Schema] =
    for {
      response <- IO(resolver.lookupSchema(igluUri))
      schema <- Utils
        .fromValidation(response)
        .leftMap(errors => MutatorError(errors.toList.mkString(", ")))
        .map(fromJsonNode)
        .flatMap(json => Schema.parse(json).liftTo[Either[MutatorError, ?]](invalidSchema(igluUri)))
        .fold(IO.raiseError[Schema], IO.pure)
    } yield schema
}

object Mutator {

  type ModelGroup = (String, String, String, Int)

  /**
    * Source of truth for mutator
    * @param fields list of BigQuery columns in order they were added
    * @param schemas all schemas known to mutator,
    *                some schemas can be not represented in `fields` if mutator started with newer versions
    */
  case class MutatorState(fields: Vector[Field], schemas: Map[ModelGroup, List[SchemaKey]]) {
    def addField(field: Field): MutatorState =
      MutatorState(fields :+ field, schemas)

    def addSchema(key: SchemaKey): MutatorState = {
      val group = (key.vendor, key.name, key.format, key.version.getModel.get)
      schemas.get(group) match {
        case Some(keys) if keys.contains(key) => this
        case Some(keys) => this.copy(schemas = schemas ++ Map(group -> (key :: keys)))
        case None => this.copy(schemas = schemas ++ Map(group -> List(key)))
      }
    }
  }

  case class MutatorError(message: String) extends Throwable

  def invalidSchema(shcema: IgluUri) = MutatorError(s"Schema [$shcema] cannot be parsed")

  def filterFields(existingFields: Vector[SchemaKey], newItems: List[InventoryItem]): List[InventoryItem] =
    newItems.filter { i =>
      SchemaKey.fromUri(i.igluUri) match {
        case Some(key) => !existingFields.contains(key)   // TODO: this should be "less" than key
        case None => false
      }
    }

  def initialize(env: Environment)(implicit F: Concurrent[IO]): IO[Either[String, Mutator]] = {
    for {
      client <- TableReference.BigQueryTable.getClient
      table = new TableReference.BigQueryTable(client, env.config.datasetId, env.config.tableId)
      fields <- table.getFields
      state <- MVar.of[IO, Vector[Field]](fields)
    } yield new Mutator(env.resolver, table, state).asRight
  }
}
