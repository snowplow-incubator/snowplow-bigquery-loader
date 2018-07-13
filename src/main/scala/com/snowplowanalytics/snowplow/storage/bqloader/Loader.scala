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
package com.snowplowanalytics.snowplow.storage.bqloader

import com.google.api.services.bigquery.model.TableReference

import org.joda.time.Duration

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.values.{SCollection, WindowOptions}

import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.compact

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import com.snowplowanalytics.snowplow.storage.bqloader.Utils.LoaderRow
import com.snowplowanalytics.snowplow.storage.bqloader.core.Config.Environment

object Loader {
  /** Default windowing option */
  val windowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

//  type TableDestination = _
//
//  val destination: DynamicDestinations[(TableDestination, TableRow), _] = ???

  /** Default BigQuery output options */
  val output: BigQueryIO.Write[TableRow] =
    BigQueryIO.writeTableRows()
      .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
      .withFormatFunction(SerializableFunctions.identity())
      .withTriggeringFrequency(Duration.millis(20000))
      .withNumFileShards(10)
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)

  /** Run whole pipeline */
  def run(env: Environment, sc: ScioContext): Unit = {
    val tableRef = new TableReference()
      .setProjectId(env.config.projectId)
      .setDatasetId(env.config.datasetId)
      .setTableId(env.config.tableId)

    val events = readEnrichedSub(sc.pubsubSubscription[String](env.config.input))

    // Should be global window with `mutateTable` invocation on every new addition
    Loader.getTypes(events).saveAsPubsub(env.config.typesTopic)

    events
      .map(_.atomic)
      .saveAsCustomOutput("bigquery", Loader.output.to(tableRef))
  }

  /** Read raw enriched TSV, parse and apply windowing */
  def readEnrichedSub(rows: SCollection[String]): SCollection[LoaderRow] =
    rows
      .flatMap(Utils.parse)
      .withFixedWindows(Duration.millis(10000), options = windowOptions)

  /** Read enriched events, produce stream of payloads */
  def getTypes(events: SCollection[LoaderRow]): SCollection[String] =
    events
      .map(_.inventory)
      .aggregate(Set.empty[InventoryItem])((acc, cur) => { acc ++ cur }, _ ++ _)
      .map(inventorySet => compact(inventorySet.map(Utils.toPayload).toList))
}
