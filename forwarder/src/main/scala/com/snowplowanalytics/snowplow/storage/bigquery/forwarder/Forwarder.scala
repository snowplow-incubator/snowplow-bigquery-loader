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

import org.joda.time.Duration

import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ScioContext

import org.joda.time.Duration
import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ScioContext
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.snowplowanalytics.snowplow.storage.bigquery.forwarder.ForwarderCli.ForwarderEnvironment

object Forwarder {

  val OutputWindow: Duration =
    Duration.millis(10000)

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  def run(env: ForwarderEnvironment, sc: ScioContext): Unit = {
    val input = PubsubIO.readStrings().fromSubscription(env.getFullFailedInsertsSub)
    sc.customInput("failedInserts", input)
      .withFixedWindows(OutputWindow, options = OutputWindowOptions)
      .saveAsCustomOutput("bigquery", getOutput.to(getTableReference(env)))
  }

  def getTableReference(env: ForwarderEnvironment): String =
    s"${env.common.config.projectId}:${env.common.config.datasetId}.${env.common.config.tableId}"

  def getOutput: BigQueryIO.Write[String] =
    BigQueryIO.write()
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      .withFormatFunction(SerializeJsonRow)  
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
      .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
}
