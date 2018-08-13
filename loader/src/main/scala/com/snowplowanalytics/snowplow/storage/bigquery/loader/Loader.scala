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
import org.json4s.JValue
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
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.values.PCollection
import org.slf4j.LoggerFactory

object Loader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val OutputWindow: Duration =
    Duration.millis(300000)

  /** Bad rows are grouped in memory, hence smaller window */
  val BadOutputWindow: Duration =
    Duration.millis(30000)

  /** Default windowing option */
  val windowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)


  val typesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  val badOutput: SideOutput[BadRow.InvalidRow] =
    SideOutput[BadRow.InvalidRow]()

  /** Default BigQuery output options */
  def getOutput(stream: Boolean): BigQueryIO.Write[LoaderRow] = {
    val common =
      BigQueryIO.write()
        .withFormatFunction(SerializeLoaderRow)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
    if (stream)
      common
        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
    else
      common
        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
        .withNumFileShards(50)
        .withTriggeringFrequency(OutputWindow)
  }

  /** Run whole pipeline */
  def run(env: Environment, sc: ScioContext): Unit = {
    val tableRef = new TableReference()
      .setProjectId(env.config.projectId)
      .setDatasetId(env.config.datasetId)
      .setTableId(env.config.tableId)

    // Raw enriched TSV
    val rows = readEnrichedSub(env.original, sc.pubsubSubscription[String](env.config.getFullInput))

    val (mainOutput, sideOutputs) = rows.withSideOutputs(typesOutput, badOutput).flatMap {
      case (Right(row), ctx) =>
        ctx.output(typesOutput, row.inventory)
        logger.warn(s"Good row $row")
        Some(row)
      case (Left(row), ctx) =>
        logger.warn(s"Bad row $row")
        ctx.output(badOutput, row)
        None
    }

    sideOutputs(typesOutput)
      .withGlobalWindow()

    sideOutputs(typesOutput)
      .withGlobalWindow()
      .applyTransform[Set[InventoryItem]](ParDo.of(new TypesUpdater))
      .aggregate(Set.empty[InventoryItem])((acc, cur) => { acc ++ cur }, _ ++ _)
      .map(types => compact(toPayload(types)))
      .saveAsPubsub(env.config.getFullTypesTopic)

    // Sink bad rows
    sideOutputs(badOutput)
      .map(_.compact)
      .saveAsPubsub(env.config.getFullBadRowsTopic)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput
      .withFixedWindows(OutputWindow, options = windowOptions)
      .internal

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(getOutput(true).to(tableRef).expand(mainOutputInternal).getFailedInserts)
      .saveAsPubsub(env.config.getFullFailedInsertsTopic)
  }

  /** Read raw enriched TSV, parse and apply windowing */
  def readEnrichedSub(resolver: JValue, rows: SCollection[String]): SCollection[Either[BadRow.InvalidRow, LoaderRow]] =
    rows
      .map(LoaderRow.parse(resolver))
      .withFixedWindows(Duration.millis(10000), options = windowOptions)
}
