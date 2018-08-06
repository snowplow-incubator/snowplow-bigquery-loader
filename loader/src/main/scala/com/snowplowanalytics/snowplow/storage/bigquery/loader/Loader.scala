
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

import java.nio.channels.Channels

import com.google.api.services.bigquery.model.TableReference

import org.joda.time.{Duration, Instant}
import org.joda.time.format.DateTimeFormatterBuilder

import org.json4s.JValue
import org.json4s.jackson.JsonMethods.compact

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.values.{SCollection, SideOutput, WindowOptions}

import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import common.Config._
import common.Codecs.toPayload

object Loader {

  val OutputWindow: Duration =
    Duration.millis(20000)

  private val DateFormatter = new DateTimeFormatterBuilder()
    .appendYear(4, 4)
    .appendLiteral('-')
    .appendMonthOfYear(2)
    .appendLiteral('-')
    .appendDayOfMonth(2)
    .toFormatter

  private val TimeFormatter = new DateTimeFormatterBuilder()
    .appendHourOfDay(2)
    .appendLiteral('-')
    .appendMinuteOfHour(2)
    .appendLiteral('-')
    .appendSecondOfMinute(2)
    .toFormatter

  /** Default windowing option */
  val windowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  val typesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  val badOutput: SideOutput[LoaderRow.BadRow] =
    SideOutput[LoaderRow.BadRow]()

  /** Default BigQuery output options */
  val output: BigQueryIO.Write[TableRow] =
    BigQueryIO.writeTableRows()
      .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
      .withFormatFunction(SerializableFunctions.identity())
      .withNumFileShards(1000)
      .withTriggeringFrequency(OutputWindow)
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)

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
        Some(row)
      case (Left(row), ctx) =>
        ctx.output(badOutput, row)
        None
    }

    val badOutputPath = env.config.badOutput

    sideOutputs(typesOutput)
      .withFixedWindows(Duration.millis(1000), options = windowOptions)
      .aggregate(Set.empty[InventoryItem])((acc, cur) => { acc ++ cur }, _ ++ _)
      .map(types => compact(toPayload(types)))
      .saveAsPubsub(env.config.getFullTypesTopic)

    sideOutputs(badOutput)
      .map(_.compact)
      .timestampBy { _ => Instant.now() }
      .withFixedWindows(OutputWindow, options = windowOptions)
      .withWindow[IntervalWindow]
      .swap
      .groupByKey
      .map(saveBadOutput(badOutputPath))

    mainOutput
      .withFixedWindows(OutputWindow, options = windowOptions)
      .map(_.data)
      .saveAsCustomOutput("bigquery", Loader.output.to(tableRef))
  }

  /** Read raw enriched TSV, parse and apply windowing */
  def readEnrichedSub(resolver: JValue, rows: SCollection[String]): SCollection[Either[LoaderRow.BadRow, LoaderRow]] =
    rows
      .map(LoaderRow.parse(resolver))
      .withFixedWindows(Duration.millis(10000), options = windowOptions)

  def saveBadOutput(bucket: String)(windowedData: (IntervalWindow, Iterable[String])): Unit = {
    val (window, badRows) = windowedData
    val trimmedBucket = if (bucket.endsWith("/")) bucket.dropRight(1) else bucket
    val start = window.start()
    val outputShard = s"$trimmedBucket/${DateFormatter.print(start)}_${TimeFormatter.print(start)}-${TimeFormatter.print(window.end())}"
    val resourceId = FileSystems.matchNewResource(outputShard, false)
    val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
    badRows.foreach { row => out.write(s"$row\n".getBytes) }
    out.close()
  }
}
