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
package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks

import scala.collection.JavaConverters._
import cats.effect.Sync
import cats.syntax.all._
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, TableId}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub.WriteTableRow
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow

object Bigquery {
  def insert[F[_]: Sync](client: BigQuery, loaderRow: LoaderRow)(env: Environment): F[Unit] = {
    val request = buildRequest(env.config.datasetId, env.config.tableId, loaderRow)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) if response.hasErrors =>
        loaderRow.data.setFactory(new JacksonFactory)
        val tableRow = loaderRow.data.toString
        println("1")                                                                         // Just to show that we get to here
        PubSub.sink(env.config.projectId, env.config.failedInserts)(WriteTableRow(tableRow)) // This appears identical to the badSink but does not work as expected
      case Right(_)    => ()
      case Left(error) => println(error)
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRow: LoaderRow) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table)).addRow(loaderRow.data).build()
}