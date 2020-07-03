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

import cats.effect.{IO, Sync}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, TableId}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub.PubSubOutput
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.Fs2LoaderRow

object Bigquery {
  def insert(loaderRow: Fs2LoaderRow, env: Environment, client: BigQuery): IO[Unit] = {
    val request = buildRequest(env.config.datasetId, env.config.tableId, loaderRow)
    Sync[IO].delay(client.insertAll(request)).attempt.flatMap {
      case Right(response) if response.hasErrors =>
        loaderRow.data.setFactory(new JacksonFactory)
        val tableRow = loaderRow.data.toString
        // TODO: Can this be turned into a sink?
        PubSub.write(PubSubOutput.WriteTableRow(tableRow), env.config.projectId, env.config.failedInserts)
      case Right(_)    => IO.unit
      case Left(error) => IO.delay(println(error))
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRow: Fs2LoaderRow) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table)).addRow(loaderRow.data).build()
}
