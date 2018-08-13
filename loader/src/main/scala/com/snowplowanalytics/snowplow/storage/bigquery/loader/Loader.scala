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
import com.spotify.scio.values.{SCollection, SideOutput, SideOutputCollections, WindowOptions}
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import common.Config._
import common.Codecs.toPayload
import org.apache.beam.sdk.transforms.Combine
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

  // Main output
  val typesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  val globalTypesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()
  val localTypesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  val badRowsOutput: SideOutput[BadRow.InvalidRow] =
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

    val (mainOutput, sideOutputs) = rows.withSideOutputs(typesOutput, badRowsOutput).flatMap {
      case (Right(row), ctx) =>
        ctx.output(typesOutput, row.inventory)
        logger.warn(s"Good row $row")
        Some(row)
      case (Left(row), ctx) =>
        logger.warn(s"Bad row $row")
        ctx.output(badRowsOutput, row)
        None
    }

    splitTypes(env.config.getFullTypesTopic)(sideOutputs)

    // Sink bad rows
    sideOutputs(badRowsOutput)
      .map(_.compact)
      .saveAsPubsub(env.config.getFullBadRowsTopic)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput
      .internal

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(getOutput(true).to(tableRef).expand(mainOutputInternal).getFailedInserts)
      .saveAsPubsub(env.config.getFullFailedInsertsTopic)
  }

  /** Read raw enriched TSV, parse and apply windowing */
  def readEnrichedSub(resolver: JValue, rows: SCollection[String]): SCollection[Either[BadRow.InvalidRow, LoaderRow]] =
    rows.map(LoaderRow.parse(resolver))
      .withFixedWindows(Duration.millis(5000), options = windowOptions)

  /**
    * Save all observed types in global window and fire only when new types arrives
    * Doesn't work :(
    */
  def splitTypes(typesTopic: String)(sideOutputs: SideOutputCollections) = {
    val (local, secondSideOutput) = sideOutputs(typesOutput)
      .withSideOutputs(globalTypesOutput)
      .flatMap { case (types, ctx) =>
        ctx.output(globalTypesOutput, types)
        Some(types)
      }

    val globalTypesView = secondSideOutput(globalTypesOutput)
      .withFixedWindows(Duration.millis(60000), options = windowOptions)
      .aggregate(Set.empty[InventoryItem])(_ ++ _, _ ++ _)
      .asIterableSideInput

    local
      .withSideInputs(globalTypesView)
      .flatMap { case (types, context) =>
        val global = context(globalTypesView).flatten.toSet
        val newElements = types -- global
        if (newElements.isEmpty) None else Some(newElements)
      }
      .toSCollection
      .filter(_.nonEmpty)
      .map { types => compact(toPayload(types)) }
      .saveAsPubsub(typesTopic)
  }
}
