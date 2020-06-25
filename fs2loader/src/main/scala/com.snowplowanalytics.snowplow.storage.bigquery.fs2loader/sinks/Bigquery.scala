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
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, TableId}
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow

object Bigquery {
  def insert[F[_]: Sync](client: BigQuery, dataset: String, table: String, loaderRow: LoaderRow): F[Unit] = {
    val request = buildRequest(dataset, table, loaderRow)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) if response.hasErrors =>
        val errors = response.getInsertErrors.asScala.toList
        println(errors)
      case Right(_)    => ()
      case Left(error) => println(error)
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRow: LoaderRow) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table)).addRow(loaderRow.data).build()
}
