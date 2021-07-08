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

import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Output
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics

import cats.effect.Sync
import cats.implicits._
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, InsertAllResponse, TableId}
import com.permutive.pubsub.producer.PubsubProducer

object Bigquery {

  final case class FailedInsert(tableRow: String) extends AnyVal

  /**
    * Insert row into BigQuery or into "failed inserts" PubSub topic if BQ client returns
    * any known errors.
    *
    * @param resources Allocated clients (BQ and PubSub)
    * @param loaderRow Parsed row to insert
    */
  def insert[F[_]: Sync](
    failedInsertProducer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    loaderRow: LoaderRow
  )(
    insertRow: LoaderRow => F[InsertAllResponse]
  ): F[Unit] =
    insertRow(loaderRow).attempt.flatMap {
      case Right(response) if response.hasErrors =>
        loaderRow.data.setFactory(new JacksonFactory)
        val tableRow = loaderRow.data.toString
        failedInsertProducer.produce(FailedInsert(tableRow)).void *> metrics.failedInsertCount
      case Right(_) =>
        metrics.latency(loaderRow.collectorTstamp.getMillis) *> metrics.goodCount
      case Left(error) => Sync[F].delay(println(error))
    }

  def mkInsert[F[_]: Sync](good: Output.BigQuery, bigQuery: BigQuery): LoaderRow => F[InsertAllResponse] = { lr =>
    val request = buildRequest(good.datasetId, good.tableId, lr)
    Sync[F].delay(bigQuery.insertAll(request))
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRow: LoaderRow) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table)).addRow(loaderRow.data).build()
}
