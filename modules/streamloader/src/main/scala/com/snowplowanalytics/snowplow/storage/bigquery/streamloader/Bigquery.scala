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
import com.permutive.pubsub.producer.Model.SimpleRecord
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, InsertAllResponse, TableId}
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.permutive.pubsub.producer.PubsubProducer

import scala.jdk.CollectionConverters.IterableHasAsJava

object Bigquery {

  final case class FailedInsert(tableRow: String) extends AnyVal

  /**
    * Insert row into BigQuery or into "failed inserts" PubSub topic if BQ client returns
    * any known errors.
    */
  def insert[F[_]: Sync](
    failedInsertProducer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    loaderRows: List[LoaderRow]
  )(
    mkInsert: List[LoaderRow] => F[InsertAllResponse]
  ): F[Unit] =
    mkInsert(loaderRows).attempt.flatMap {
      case Right(response) if response.hasErrors =>
        val errorIndex = response.getInsertErrors.keySet()
        val failed     = loaderRows.zipWithIndex.filter { case (_, i) => errorIndex.contains(i.toLong) }.map(_._1)
        val tableRows  = failed.map(lr => SimpleRecord(FailedInsert(lr.toString)))
        failedInsertProducer.produceMany[List](tableRows).void *> metrics.failedInsertCount(tableRows.length)
      case Right(_) =>
        val earliestCollectorTstamp = loaderRows.map(_.collectorTstamp).min.getMillis
        metrics.latency(earliestCollectorTstamp) *> metrics.goodCount(loaderRows.length)
      case Left(errors) => Sync[F].delay(println(errors))
    }

  def mkInsert[F[_]: Sync](good: Output.BigQuery, bigQuery: BigQuery): List[LoaderRow] => F[InsertAllResponse] = {
    lrs =>
      val request = buildRequest(good.datasetId, good.tableId, lrs)
      Sync[F].delay(bigQuery.insertAll(request))
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, loaderRows: List[LoaderRow]) = {
    val tableRows = loaderRows.map(lr => RowToInsert.of(lr.data)).asJava
    InsertAllRequest.newBuilder(TableId.of(dataset, table), tableRows).build()
  }
}
