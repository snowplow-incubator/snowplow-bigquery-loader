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

import org.joda.time.Duration
import org.json4s.jackson.JsonMethods.compact
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideOutput, WindowOptions}
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.util.Transport
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import common.Config._
import common.Codecs.toPayload
import implicits._
import org.json4s.JsonAST.JValue


object Loader {

  private val OutputSeconds = 10

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  /** Side output for shredded types */
  val ObservedTypesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  /** Emit observed types every 10 minutes / workers */
  val TypesWindow: Duration =
    Duration.millis(60000)

  /** Side output for rows failed transformation */
  val BadRowsOutput: SideOutput[BadRow] =
    SideOutput[BadRow]()

  @transient lazy val jsonFactory = Transport.getJsonFactory

  def splitInput(sc: ScioContext, input: PubsubIO.Read[String], resolverJson: JValue) = {
    sc.customInput("input", input)
      .applyTransform(Window.into(FixedWindows.of(Duration.standardSeconds(OutputSeconds))))
      .withName("windowed")
      .map(LoaderRow.parse(resolverJson))
      .withSideOutputs(ObservedTypesOutput, BadRowsOutput)
      .withName("splitGoodBad")
      .flatMap {
        case (Right(row), ctx) =>
          ctx.output(ObservedTypesOutput, row.inventory)
          Some(row)
        case (Left(row), ctx) =>
          ctx.output(BadRowsOutput, row)
          None
      }

  }

  /** Run whole pipeline */
  def run(env: ValueProvider[Environment], sc: ScioContext): Unit = {
    val input = PubsubIO.readStrings().fromSubscription(env.map(_.config.getFullInput))
    val typesSink = PubsubIO.writeStrings().to(env.map(_.config.getFullTypesTopic))
    val badRowsSink = PubsubIO.writeStrings().to(env.map(_.config.getFullBadRowsTopic))
    val failedInsertsSink = PubsubIO.writeStrings().to(env.map(_.config.getFullFailedInsertsTopic))
    lazy val resolverJson = env.map(_.resolverJson).get()

    val (mainOutput, sideOutputs) = splitInput(sc, input, resolverJson)

    // Emit all types observed in 1 minute
    aggregateTypes(sideOutputs(ObservedTypesOutput))
      .saveAsCustomOutput("typesSink", typesSink)

    // Sink bad rows
    sideOutputs(BadRowsOutput)
      .map(_.compact)
      .withName("badRowsCompact")
      .saveAsCustomOutput("badRowsSink", badRowsSink)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput.internal
    val output = getOutput
      .to(env.map(getTableReference))
      .expand(mainOutputInternal).getFailedInserts

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(output)
      .map(row => jsonFactory.toString(row))
      .withName("tableRowToJson")
      .saveAsCustomOutput("failedInsertsSink", failedInsertsSink)
  }

  /** Default BigQuery output options */
  def getOutput: BigQueryIO.Write[LoaderRow] =
    BigQueryIO.write()
      .withFormatFunction(SerializeLoaderRow)
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
      .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())

  /** Group types into smaller chunks */
  def aggregateTypes(types: SCollection[Set[InventoryItem]]): SCollection[String] =
    types
      .withFixedWindows(TypesWindow, options = OutputWindowOptions)
      .withName("windowedTypes")
      .aggregate(Set.empty[InventoryItem])(_ ++ _, _ ++ _)
      .withName("aggregateTypes")
      .withWindow[IntervalWindow]
      .withName("withIntervalWindow")
      .swap
      .groupByKey
      .map { case (_, groupedSets) => groupedSets.toSet.flatten }
      .filter(_.nonEmpty)
      .withName("filterNonEmpty")
      .map { types => compact(toPayload(types)) }

  def getTableReference(env: Environment): String =
    s"${env.config.projectId}:${env.config.datasetId}.${env.config.tableId}"
}
