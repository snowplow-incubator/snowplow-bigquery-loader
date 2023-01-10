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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Mode, Field => DdlField}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.storage.bigquery.common.{Adapter, LoaderRow, Schema => LoaderSchema}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.MutatorEnvironment

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import com.google.cloud.bigquery.Field
import io.circe.Json

import fs2.{Pipe, Stream}

import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * Mutator is stateful worker that emits `alter table` requests.
  * It does not depend on any source of requests (such as PubSub topic)
  *
  * @param resolver iglu resolver, responsible for fetching schemas
  * @param tableReference object responsible for table interactions
  * @param state current state of the table, source of truth
  */
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

  private def mkResolver(igluConfig: Json): EitherT[IO, String, Resolver[IO]] =
    EitherT
      .fromEither[IO](Resolver.parseConfig(igluConfig))
      .flatMap(conf => Resolver.fromConfig[IO](conf))
      .leftMap(failure => failure.show)

  def initialize(
    env: MutatorEnvironment,
    verbose: Boolean
  )(implicit c: Concurrent[IO], logger: Logger[IO]): EitherT[IO, String, Pipe[IO, List[ShreddedType], Unit]] =
    for {
      bqClient <- EitherT.liftF(TableReference.BigQueryTable.getClient(env.projectId))
      table = new TableReference.BigQueryTable(
        bqClient,
        env.config.output.good.datasetId,
        env.config.output.good.tableId
      )
      fields   <- EitherT.liftF(table.getFields)
      resolver <- mkResolver(env.resolverJson)
      _        <- EitherT.liftF[IO, String, Unit](addField(table, LoaderRow.LoadTstampField))
      ref      <- EitherT.liftF(Ref.of(MutatorState(fields, 0)))
    } yield pipe(resolver, table, ref, verbose)

  def pipe(resolver: Resolver[IO], tableReference: TableReference, ref: Ref[IO, MutatorState], verbose: Boolean)(
    implicit logger: Logger[IO]
  ): Pipe[IO, List[ShreddedType], Unit] =
    in =>
      for {
        inventoryItems <- in
        _ <- Stream.eval(
          if (verbose)
            logger.info(
              s"Received ${inventoryItems.map(x => s"${x.shredProperty} ${x.schemaKey.toSchemaUri}").mkString(", ")}"
            )
          else IO.unit
        )
        state <- Stream.eval(ref.get)
        existingColumns = state.fields.map(_.getName)
        fieldsToAdd     = filterFields(existingColumns, inventoryItems)
        state <- Stream.eval(fieldsToAdd.foldLeftM(state)(addShreddedType(tableReference, resolver, _, _)))
        state <- Stream.emit(state.increment)
        _     <- Stream.eval(if (state.received % 100 == 0) logState(state) else IO.unit)
        _     <- Stream.eval(ref.set(state))
      } yield ()

  /** Perform ALTER TABLE */
  def addShreddedType(
    tableReference: TableReference,
    resolver: Resolver[IO],
    state: MutatorState,
    inventoryItem: ShreddedType
  )(implicit logger: Logger[IO]): IO[MutatorState] = {
    val result = for {
      schema <- getSchema(resolver, inventoryItem.schemaKey)
      field = getField(inventoryItem, schema)
      _ <- tableReference.addField(field)
      _ <- logger.info(s"Added ${field.getName}")
    } yield state.add(field)

    result.recoverWith(logAddItem(inventoryItem).andThen(_.as(state))).timeout(30.seconds)
  }

  /** Add only one field to table */
  def addField(tableReference: TableReference, field: DdlField)(implicit logger: Logger[IO]): IO[Unit] =
    for {
      fields <- tableReference.getFields
      contains    = fields.exists(_.getName == field.name)
      addFieldOps = tableReference.addField(Adapter.adaptField(field))
      _ <- if (contains) logger.info(s"${field.name} already exists")
      else addFieldOps *> logger.info(s"Added ${field.name}")
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
  def getSchema(resolver: Resolver[IO], key: SchemaKey)(implicit logger: Logger[IO]): IO[Schema] = {
    val action = for {
      response <- EitherT(resolver.lookupSchema(key).timeout(10.seconds)).leftMap(fetchError)
      _        <- EitherT.liftF(logger.info(s"Received schema from Iglu registry: ${response.noSpaces}"))
      schema   <- EitherT.fromOption[IO](Schema.parse(response), invalidSchema(key))
    } yield schema

    action.value.flatMap(IO.fromEither)
  }

  private def logState(mutatorState: MutatorState)(implicit logger: Logger[IO]): IO[Unit] =
    if (mutatorState.received == Long.MaxValue - 2) {
      logger.info(
        s"received ${mutatorState.received} records, resetting counted; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}"
      )
    } else {
      logger.info(
        s"received ${mutatorState.received} records; known fields are ${mutatorState.fields.map(_.getName).mkString(", ")}"
      )
    }

  private def logAddItem(item: Data.ShreddedType)(implicit logger: Logger[IO]): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(error) =>
      logger.error(error)(s"Failed to add ${item.schemaKey.toSchemaUri} as ${item.shredProperty.name}")
  }

  private def invalidSchema(schema: SchemaKey) =
    MutatorError(s"Schema [${schema.toSchemaUri}] cannot be parsed")

  private def fetchError(error: ClientError.ResolutionError) = // TODO: actual implementation
    MutatorError(s"Schema [$error] cannot be parsed")
}
