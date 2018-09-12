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
package forwarder

import com.google.api.services.bigquery.model.TableReference
import org.joda.time.Duration

import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ScioContext

import com.snowplowanalytics.snowplow.storage.bigquery.forwarder.CommandLine.ForwarderEnvironment

import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

object Forwarder {

  val OutputWindow: Duration =
    Duration.millis(10000)

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  def run(env: ForwarderEnvironment, sc: ScioContext): Unit = {
    val tableRef = new TableReference()
      .setProjectId(env.common.config.projectId)
      .setDatasetId(env.common.config.datasetId)
      .setTableId(env.common.config.tableId)

    val output: BigQueryIO.Write[TableRow] =
      BigQueryIO.writeTableRows()
        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .to(tableRef)

    sc.pubsubSubscription[TableRow](env.getFullFailedInsertsSub)
      .withFixedWindows(OutputWindow, options = OutputWindowOptions)
      .saveAsCustomOutput(env.common.config.tableId, output)
  }
}
