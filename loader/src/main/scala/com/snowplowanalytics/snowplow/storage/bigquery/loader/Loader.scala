/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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

import org.slf4j.LoggerFactory
import com.google.api.services.bigquery.model.TableReference
import io.circe.Json
import org.joda.time.{Duration, Instant}
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideOutput, WindowOptions}
import com.spotify.scio.coders.Coder
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import scala.collection.JavaConverters._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderCli.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.loader.metrics._
import common.Config._
import common.Codecs.toPayload

object Loader {

  implicit val coderBadRow: Coder[BadRow] = Coder.kryo[BadRow]

  val OutputWindow: Duration =
    Duration.millis(10000)

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  /** Side output for shredded types */
  val ObservedTypesOutput: SideOutput[Set[ShreddedType]] =
    SideOutput[Set[ShreddedType]]()

  /** Emit observed types every 10 minutes / workers */
  val TypesWindow: Duration =
    Duration.millis(60000)

  /** Side output for rows failed transformation */
  val BadRowsOutput: SideOutput[BadRow] =
    SideOutput[BadRow]()

  /** Run whole pipeline */
  def run(env: LoaderEnvironment, sc: ScioContext): Unit = {
    if (env.labels.nonEmpty) {
      sc.optionsAs[DataflowPipelineOptions].setLabels(env.labels.asJava)
    }

    val (mainOutput, sideOutputs) = getData(env.common.resolverJson, sc, env.common.config.getFullInput)
      .withSideOutputs(ObservedTypesOutput, BadRowsOutput)
      .withName("splitGoodBad")
      .flatMap {
        case (Right(row), ctx) =>
          ctx.output(ObservedTypesOutput, row.inventory)
          val diff = Instant.now().getMillis - row.collectorTstamp.getMillis
          latency.update(diff) match {
            case Right(upd) => upd
            // We can get a Left if an exception is thrown.
            case Left(e) => LoggerFactory.getLogger("non-fatal").debug("Non-fatal exception:\n", e)
          }
          Some(row)
        case (Left(row), ctx) =>
          ctx.output(BadRowsOutput, row)
          None
      }

    // Emit all types observed in 1 minute
    aggregateTypes(sideOutputs(ObservedTypesOutput))
      .saveAsPubsub(env.common.config.getFullTypesTopic)

    // Sink bad rows
    sideOutputs(BadRowsOutput)
      .map(_.compact)
      .saveAsPubsub(env.common.config.getFullBadRowsTopic)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput.internal
    val remaining = getOutput(env.common.config.load)
      .to(getTableReference(env.common))
      .expand(mainOutputInternal).getFailedInserts

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(remaining)
      .withName("failedInsertsSink")
      .saveAsPubsub(env.common.config.getFullFailedInsertsTopic)
  }

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
        if (retry) {
          streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
        } else {
          streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
        }
      case LoadMode.FileLoads(frequency) =>
        common
          .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
          .withNumFileShards(100)
          .withTriggeringFrequency(Duration.standardSeconds(frequency))
    }
  }

  /** Group types into smaller chunks */
  def aggregateTypes(types: SCollection[Set[ShreddedType]]): SCollection[String] =
    types
      .withFixedWindows(TypesWindow, options = OutputWindowOptions)
      .withName("aggregateTypes")
      .aggregate(Set.empty[ShreddedType])(_ ++ _, _ ++ _)
      .withName("withIntervalWindow")
      .withWindow[IntervalWindow]
      .swap
      .groupByKey
      .map { case (_, groupedSets) => groupedSets.toSet.flatten }
      .withName("filterNonEmpty")
      .filter(_.nonEmpty)
      .map { types => toPayload(types).noSpaces }

  /** Read data from PubSub topic and transform to ready to load rows */
  def getData(resolver: Json,
              sc: ScioContext,
              input: String): SCollection[Either[BadRow, LoaderRow]] =
    sc.pubsubSubscription[String](input)
      .map(LoaderRow.parse(resolver))
      .withFixedWindows(OutputWindow, options = OutputWindowOptions)

  def getTableReference(env: Environment): TableReference =
    new TableReference()
      .setProjectId(env.config.projectId)
      .setDatasetId(env.config.datasetId)
      .setTableId(env.config.tableId)
}
