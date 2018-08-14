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

import com.google.api.services.bigquery.model.TableReference

import org.joda.time.Duration

import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.compact

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideOutput, WindowOptions}

import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import common.Config._
import common.Codecs.toPayload

object Loader {

  /** Neither loading API work with global windows */
  val OutputWindow: Duration =
    Duration.millis(5000)

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  /** Side output for shredded types */
  val ObservedTypesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  /** Emit observed types every minute */
  val TypesWindow: Duration =
    Duration.standardSeconds(60)

  /** Side output for rows failed transformation */
  val BadRowsOutput: SideOutput[BadRow] =
    SideOutput[BadRow]()

  /** Run whole pipeline */
  def run(env: Environment, sc: ScioContext): Unit = {
    val tableRef = new TableReference()
      .setProjectId(env.config.projectId)
      .setDatasetId(env.config.datasetId)
      .setTableId(env.config.tableId)

    val (mainOutput, sideOutputs) = getData(env.resolverJson, sc, env.config.getFullInput)
      .withSideOutputs(ObservedTypesOutput, BadRowsOutput)
      .flatMap {
        case (Right(row), ctx) =>
          ctx.output(ObservedTypesOutput, row.inventory)
          Some(row)
        case (Left(row), ctx) =>
          ctx.output(BadRowsOutput, row)
          None
      }

    // Emit all types observed in 1 minute
    sideOutputs(ObservedTypesOutput)
      .withFixedWindows(TypesWindow)
      .aggregate(Set.empty[InventoryItem])(_ ++ _, _ ++ _)
      .filter(_.nonEmpty)
      .map { types => compact(toPayload(types)) }
      .saveAsPubsub(env.config.getFullTypesTopic)

    // Sink bad rows
    sideOutputs(BadRowsOutput)
      .map(_.compact)
      .saveAsPubsub(env.config.getFullBadRowsTopic)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput.internal

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(getOutput(env.config.load).to(tableRef).expand(mainOutputInternal).getFailedInserts)
      .saveAsPubsub(env.config.getFullFailedInsertsTopic)
  }

  /** Read data from PubSub topic and transform to ready to load rows */
  def getData(resolver: JValue,
              sc: ScioContext,
              input: String): SCollection[Either[BadRow, LoaderRow]] =
    sc.pubsubSubscription[String](input)
      .map(LoaderRow.parse(resolver))
      .withFixedWindows(OutputWindow, options = OutputWindowOptions)

  /** Default BigQuery output options */
  def getOutput(loadMode: LoadMode): BigQueryIO.Write[LoaderRow] = {
    val common =
      BigQueryIO.write()
        .withFormatFunction(SerializeLoaderRow)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)

    loadMode match {
      case LoadMode.StreamingInserts(retry) =>
        val streaming = common.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        if (retry) streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
        else streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
      case LoadMode.FileLoads(frequency) =>
        common
          .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
          .withNumFileShards(50)
          .withTriggeringFrequency(Duration.standardSeconds(frequency))
    }
  }
}
