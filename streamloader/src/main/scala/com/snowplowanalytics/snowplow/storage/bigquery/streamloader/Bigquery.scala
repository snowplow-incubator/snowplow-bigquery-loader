/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, TableId}

import cats.effect.{IO, Sync}

import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow

object Bigquery {

  final case class FailedInsert(tableRow: String) extends AnyVal

  /**
    * Insert row into BigQuery or into "failed inserts" PubSub topic if BQ client returns
    * any known errors.
    *
    * @param resources Allocated clients (BQ and PubSub)
    * @param loaderRow Parsed row to insert
    */
  def insert(resources: Resources[IO], loaderRow: LoaderRow): IO[Unit] = {
    val request =
      buildRequest(resources.env.config.output.good.datasetId, resources.env.config.output.good.tableId, loaderRow)
    IO.delay(resources.bigQuery.insertAll(request)).attempt.flatMap {
      case Right(response) if response.hasErrors =>
        loaderRow.data.setFactory(new JacksonFactory)
        val tableRow = loaderRow.data.toString
        resources.failedInsertsProducer.produce(FailedInsert(tableRow)).void
      case Right(_)    => IO.unit
      case Left(error) => IO.delay(println(error))
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRow: LoaderRow) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table)).addRow(loaderRow.data).build()
}
