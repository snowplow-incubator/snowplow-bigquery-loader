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

import java.time.Instant

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.MVar

import org.json4s.jackson.JsonMethods.fromJsonNode

import com.google.cloud.bigquery.Field
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Generator, Mode}

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.{IgluUri, InventoryItem}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data

import common.{Adapter, Utils, Schema => LoaderSchema}
import common.Config._
import Mutator._

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
                      state: MVar[IO, MutatorState],
                      verbose: Boolean) {

  /** Add all new columns to a table */
  def updateTable(inventoryItems: List[InventoryItem]): IO[Unit] =
    for {
      _                       <- if (verbose) log(s"Received ${inventoryItems.map(x => s"${x.shredProperty} ${x.igluUri}").mkString(", ")}") else IO.unit
      MutatorState(fields, _) <- state.read
      existingColumns          = fields.map(_.getName)
      fieldsToAdd              = filterFields(existingColumns, inventoryItems)
      _                       <- fieldsToAdd.traverse_(addField)
      latestState             <- state.take
      _                       <- state.put(latestState.increment)
      _                       <- if (latestState.received % 100 == 0) log(latestState) else IO.unit
    } yield ()

  /** Perform ALTER TABLE */
  def addField(inventoryItem: InventoryItem): IO[Unit] =
    for {
      schema <- getSchema(inventoryItem.igluUri)
      field   = getField(inventoryItem, schema)
      _      <- tableReference.addField(field)
      st     <- state.take
      _      <- state.put(st.add(field))
      _      <- log(s"added ${field.getName}")
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
      schema   <- Utils
        .fromValidation(response)
        .leftMap(errors => MutatorError(errors.toList.mkString(", ")))
        .map(fromJsonNode)
        .flatMap(json => Schema.parse(json).liftTo[Either[MutatorError, ?]](invalidSchema(igluUri)))
        .fold(IO.raiseError[Schema], IO.pure)
    } yield schema

  private def log(message: String): IO[Unit] =
    IO(println(s"Mutator ${Instant.now()}: $message"))

  private def log(mutatorState: MutatorState): IO[Unit] = {
    if (mutatorState.received == Long.MaxValue - 2) {
      log(s"received ${mutatorState.received} records, resetting counted; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}")
    } else {
      log(s"received ${mutatorState.received} records; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}")
    }
  }
}

object Mutator {

  case class MutatorError(message: String) extends Throwable

  case class MutatorState(fields: Vector[Field], received: Long) {
    def increment: MutatorState = {
      if (received == Long.MaxValue - 1) {
        MutatorState(fields, 0)
      } else {
        MutatorState(fields, received + 1)
      }
    }

    def add(field: Field): MutatorState =
      MutatorState(fields :+ field, received)
  }

  def filterFields(existingColumns: Vector[String], newItems: List[InventoryItem]): List[InventoryItem] =
    newItems.filterNot(item => existingColumns.contains(LoaderSchema.getColumnName(item)))

  def initialize(env: Environment, verbose: Boolean)(implicit F: Concurrent[IO]): IO[Either[String, Mutator]] = {
    for {
      client <- TableReference.BigQueryTable.getClient
      table   = new TableReference.BigQueryTable(client, env.config.datasetId, env.config.tableId)
      fields <- table.getFields
      state  <- MVar.of(MutatorState(fields, 0))
    } yield new Mutator(env.resolver, table, state, verbose).asRight
  }

  private def invalidSchema(schema: IgluUri) =
    MutatorError(s"Schema [$schema] cannot be parsed")

}
