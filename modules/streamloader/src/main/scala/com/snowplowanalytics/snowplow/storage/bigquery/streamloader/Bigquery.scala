/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{BigQueryRetrySettings, Output}
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics

import cats.effect.Sync
import cats.implicits._
import com.google.api.client.json.gson.GsonFactory
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, InsertAllResponse, TableId}
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.permutive.pubsub.producer.PubsubProducer
import org.threeten.bp.Duration

import scala.jdk.CollectionConverters.IterableHasAsJava

object Bigquery {

  final case class FailedInsert(tableRow: String) extends AnyVal

  /**
    * Insert rows into BigQuery or into "failed inserts" PubSub topic if BQ client returns
    * any known errors.
    *
    * Exceptions from the underlying `insertAll` call are propagated.
    */
  def insert[F[_]: Sync](
    failedInsertProducer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    loaderRows: List[LoaderRow]
  )(
    mkInsert: List[LoaderRow] => F[InsertAllResponse]
  ): F[Unit] =
    mkInsert(loaderRows).flatMap {
      case response if response.hasErrors =>
        val errorIndex = response.getInsertErrors.keySet()
        val failed     = loaderRows.zipWithIndex.filter { case (_, i) => errorIndex.contains(i.toLong) }.map(_._1)
        val tableRows = failed.map { lr =>
          lr.data.setFactory(GsonFactory.getDefaultInstance)
          FailedInsert(lr.data.toString)
        }
        tableRows.traverse_(fi => failedInsertProducer.produce(fi)) *> metrics.failedInsertCount(tableRows.length)
      case _ =>
        val earliestCollectorTstamp = loaderRows.map(_.collectorTstamp).min.getMillis
        metrics.latency(earliestCollectorTstamp) *> metrics.goodCount(loaderRows.length)
    }

  def mkInsert[F[_]: Sync](
    good: Output.BigQuery,
    bigQuery: BigQuery
  ): List[LoaderRow] => F[InsertAllResponse] = { lrs =>
    val request = buildRequest(good.datasetId, good.tableId, lrs)
    Sync[F].blocking(bigQuery.insertAll(request))
  }

  def getClient[F[_]: Sync](rs: BigQueryRetrySettings, projectId: String): F[BigQuery] = {
    val retrySettings =
      RetrySettings
        .newBuilder()
        .setInitialRetryDelay(Duration.ofSeconds(rs.initialDelay.toLong))
        .setRetryDelayMultiplier(rs.delayMultiplier)
        .setMaxRetryDelay(Duration.ofSeconds(rs.maxDelay.toLong))
        .setTotalTimeout(Duration.ofMinutes(rs.totalTimeout.toLong))
        .build

    Sync[F].delay(
      BigQueryOptions.newBuilder.setRetrySettings(retrySettings).setProjectId(projectId).build.getService
    )
  }

  private def buildRequest(dataset: String, table: String, loaderRows: List[LoaderRow]) = {
    val tableRows = loaderRows.map(lr => RowToInsert.of(lr.data)).asJava
    InsertAllRequest.newBuilder(TableId.of(dataset, table), tableRows).build()
  }
}
