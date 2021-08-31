/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import com.snowplowanalytics.iglu.client.{Client, ClientError}
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Mode, Field => DdlField}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.storage.bigquery.common.{Adapter, Schema => LoaderSchema, LoaderRow}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.MutatorEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.mutator.Mutator._

import cats.data.EitherT
import cats.effect._
import cats.effect.concurrent.{MVar, MVar2}
import cats.implicits._
import com.google.cloud.bigquery.Field
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * Mutator is stateful worker that emits `alter table` requests.
  * It does not depend on any source of requests (such as PubSub topic)
  *
  * @param igluClient iglu resolver, responsible for fetching schemas
  * @param tableReference object responsible for table interactions
  * @param state current state of the table, source of truth
  */
class Mutator private (
  igluClient: Client[IO, Json],
  tableReference: TableReference,
  state: MVar2[IO, MutatorState],
  verbose: Boolean
) {

  /** Add all new columns to a table */
  def updateTable(inventoryItems: List[Data.ShreddedType])(implicit t: Timer[IO], cs: ContextShift[IO]): IO[Unit] =
    // format: off
    for {
      _ <- if (verbose)
      { log(s"Received ${inventoryItems.map(x => s"${x.shredProperty} ${x.schemaKey.toSchemaUri}").mkString(", ")}") }
      else { IO.unit }
      MutatorState(fields, _) <- state.read
      existingColumns = fields.map(_.getName)
      fieldsToAdd     = filterFields(existingColumns, inventoryItems)
      _           <- fieldsToAdd.traverse_(item => addField(item).recoverWith(logAddItem(item))).timeout(30.seconds)
      latestState <- state.take
      _           <- state.put(latestState.increment)
      _ <- if (latestState.received % 100 == 0) { log(latestState) } else { IO.unit }
    } yield ()
  // format: on

  /** Perform ALTER TABLE */
  def addField(inventoryItem: ShreddedType)(implicit t: Timer[IO], cs: ContextShift[IO]): IO[Unit] =
    for {
      schema <- getSchema(inventoryItem.schemaKey)
      field = getField(inventoryItem, schema)
      _  <- tableReference.addField(field)
      st <- state.take
      _  <- state.put(st.add(field))
      _  <- log(s"Added ${field.getName}")
    } yield ()

  /** Add only one field to table */
  def addField(field: DdlField): IO[Unit] =
    for {
      fields <- tableReference.getFields
      contains = fields.exists(_.getName == field.name)
      addFieldOps = tableReference.addField(Adapter.adaptField(field))
      _ <- if (contains) log(s"${field.name} already exists") else addFieldOps *> log(s"Added ${field.name}")
    } yield ()

  /** Transform Iglu Schema into valid BigQuery field */
  def getField(inventoryItem: Data.ShreddedType, schema: Schema): Field = {
    val mode = inventoryItem.shredProperty match {
      case Data.UnstructEvent => Mode.Nullable
      case Data.Contexts(_)   => Mode.Repeated
    }

    val columnName = LoaderSchema.getColumnName(inventoryItem)
    val field      = DdlField.build(columnName, schema, false).setMode(mode).normalized
    Adapter.adaptField(field)
  }

  /** Receive the JSON Schema from the Iglu registry */
  def getSchema(key: SchemaKey)(implicit t: Timer[IO], cs: ContextShift[IO]): IO[Schema] = {
    val action = for {
      response <- EitherT(igluClient.resolver.lookupSchema(key).timeout(10.seconds)).leftMap(fetchError)
      _        <- EitherT.liftF(log(s"Received $response from Iglu registry"))
      schema   <- EitherT.fromOption[IO](Schema.parse(response), invalidSchema(key))
    } yield schema

    action.value.flatMap(IO.fromEither)
  }

  private def log(message: String): IO[Unit] =
    IO(println(s"Mutator ${Instant.now()}: $message"))

  private def log(mutatorState: MutatorState): IO[Unit] =
    if (mutatorState.received == Long.MaxValue - 2) {
      log(
        s"received ${mutatorState.received} records, resetting counted; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}"
      )
    } else {
      log(
        s"received ${mutatorState.received} records; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}"
      )
    }

  private def logAddItem(item: Data.ShreddedType): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(error) =>
      IO {
        System.err.println(s"Failed to add ${item.schemaKey.toSchemaUri} as ${item.shredProperty.name}")
        error.printStackTrace()
        System.out.println("Continuing...")
      }
  }
}

object Mutator {
  case class MutatorError(message: String) extends Throwable

  case class MutatorState(fields: Vector[Field], received: Long) {
    def increment: MutatorState =
      if (received >= Long.MaxValue - 1) MutatorState(fields, 0) else MutatorState(fields, received + 1)

    def add(field: Field): MutatorState =
      MutatorState(fields :+ field, received)
  }

  def filterFields(existingColumns: Vector[String], newItems: List[ShreddedType]): List[ShreddedType] =
    newItems.filterNot(item => existingColumns.contains(LoaderSchema.getColumnName(item)))

  def initialize(env: MutatorEnvironment, verbose: Boolean)(implicit c: Concurrent[IO]): IO[Either[String, Mutator]] = {
    val mutatorIO: IO[Either[String, Mutator]] = for {
      bqClient <- TableReference.BigQueryTable.getClient
      table = new TableReference.BigQueryTable(
        bqClient,
        env.config.output.good.datasetId,
        env.config.output.good.tableId
      )
      fields     <- table.getFields
      state      <- MVar.of(MutatorState(fields, 0))
      igluClient <- Client.parseDefault[IO](env.resolverJson).value.flatMap(IO.fromEither)
    } yield new Mutator(igluClient, table, state, verbose).asRight
    val res = for {
      mutator <- EitherT(mutatorIO)
      _ <- EitherT.liftF[IO, String, Unit](mutator.addField(LoaderRow.LoadTstampField))
    } yield mutator
    res.value
  }

  private def invalidSchema(schema: SchemaKey) =
    MutatorError(s"Schema [${schema.toSchemaUri}] cannot be parsed")

  private def fetchError(error: ClientError.ResolutionError) = // TODO: actual implementation
    MutatorError(s"Schema [$error] cannot be parsed")
}
