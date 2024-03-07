/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Show
import cats.implicits._
import cats.effect.{Async, Sync}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  FieldList,
  Schema,
  StandardTableDefinition,
  Table,
  TableDefinition,
  TableInfo,
  TimePartitioning
}
import com.google.auth.Credentials

import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields
import com.snowplowanalytics.snowplow.bigquery.{Alert, AppHealth, Config, Monitoring}

import scala.jdk.CollectionConverters._

trait TableManager[F[_]] {

  def addColumns(columns: Vector[Field]): F[Unit]

  def createTable: F[Unit]

}

object TableManager {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def make[F[_]: Async](
    config: Config.BigQuery,
    retries: Config.Retries,
    credentials: Credentials,
    appHealth: AppHealth[F],
    monitoring: Monitoring[F]
  ): F[TableManager[F]] =
    for {
      client <- Sync[F].delay(BigQueryOptions.newBuilder.setCredentials(credentials).build.getService)
    } yield impl(config, retries, client, appHealth, monitoring)

  private def impl[F[_]: Async](
    config: Config.BigQuery,
    retries: Config.Retries,
    client: BigQuery,
    appHealth: AppHealth[F],
    monitoring: Monitoring[F]
  ): TableManager[F] = new TableManager[F] {

    def addColumns(columns: Vector[Field]): F[Unit] =
      BigQueryRetrying.withRetries(appHealth, retries, monitoring, Alert.FailedToAddColumns(columns.map(_.name), _)) {
        Logger[F].info(s"Altering table to add columns [${showColumns(columns)}]") *>
          addColumnsImpl(config, client, columns)
      }

    def createTable: F[Unit] =
      BigQueryRetrying.withRetries(appHealth, retries, monitoring, Alert.FailedToCreateEventsTable(_)) {
        val tableInfo = atomicTableInfo(config)
        Logger[F].info(show"Creating table $tableInfo") *>
          Sync[F]
            .blocking(client.create(tableInfo))
            .void
            .recoverWith {
              case bqe @ BigQueryUtils.BQExceptionWithLowerCaseReason("duplicate") =>
                // Table already exists
                Logger[F].info(s"Ignoring error when creating table: ${bqe.getMessage}")
              case BigQueryUtils.BQExceptionWithLowerCaseReason("accessdenied") =>
                Logger[F].info(s"Access denied when trying to create table. Will ignore error and assume table already exists.")
            }
      }
  }

  private def addColumnsImpl[F[_]: Sync](
    config: Config.BigQuery,
    client: BigQuery,
    columns: Vector[Field]
  ): F[Unit] =
    for {
      table <- Sync[F].blocking(client.getTable(config.dataset, config.table))
      schema <- Sync[F].pure(table.getDefinition[TableDefinition].getSchema)
      fields <- Sync[F].pure(schema.getFields)
      fields <- Sync[F].pure(BigQuerySchemaUtils.mergeInColumns(fields, columns))
      schema <- Sync[F].pure(Schema.of(fields))
      table <- Sync[F].pure(setTableSchema(table, schema))
      _ <- Sync[F]
             .blocking(table.update())
             .void
             .onError(logOnRaceCondition)
    } yield ()

  private def setTableSchema(table: Table, schema: Schema): Table =
    table.toBuilder().setDefinition(StandardTableDefinition.of(schema)).build()

  private def logOnRaceCondition[F[_]: Sync]: PartialFunction[Throwable, F[Unit]] = {
    case BigQueryUtils.BQExceptionWithLowerCaseReason("invalid") =>
      Logger[F].warn(s"Caught known exception which probably means another loader has already altered the table.")
    // Don't do anything else; the BigQueryRetrying will handle retries and logging the exception.
  }

  private def showColumns(columns: Vector[Field]): String =
    columns.map(_.name).mkString(", ")

  private def atomicTableInfo(config: Config.BigQuery): TableInfo = {
    val atomicFields = AtomicFields.withLoadTstamp.map(BigQuerySchemaUtils.bqFieldOf)
    val fields       = FieldList.of(atomicFields.asJava)
    val schema       = Schema.of(fields)
    val tableDefinition = StandardTableDefinition.newBuilder
      .setSchema(schema)
      .setTimePartitioning {
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField("load_tstamp")
          .build()
      }
      .build()
    TableInfo.of(BigQueryUtils.tableIdOf(config), tableDefinition)
  }

  private implicit val showTableInfo: Show[TableInfo] = Show(_.getTableId.getIAMResourceName)

}
