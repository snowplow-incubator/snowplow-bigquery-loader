/*
 * Copyright (c) 2018-2023 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.snowplow.storage.bigquery.common.{LoaderRow, createGcpUserAgentHeader}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{BigQueryRetrySettings, Output}
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import cats.effect.{Async, Sync}
import cats.implicits._
import retry.{RetryDetails, RetryPolicy, retryingOnAllErrors}
import com.google.api.client.json.gson.GsonFactory
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryException,
  BigQueryOptions,
  InsertAllRequest,
  InsertAllResponse,
  TableId
}
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.AllAppsConfig.GcpUserAgent
import org.threeten.bp.Duration
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.jdk.CollectionConverters._

object Bigquery {

  final case class FailedInsert(tableRow: String) extends AnyVal

  implicit private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  // A trait for responses from BigQuery which we know how to handle
  sealed trait HandledResponse

  object HandledResponse {
    case class Success(response: InsertAllResponse) extends HandledResponse
    case class RequestWasInvalid(bqe: BigQueryException) extends HandledResponse
  }

  /**
    * Insert rows into BigQuery or into "failed inserts" PubSub topic if BQ client returns
    * any known errors.
    *
    * Exceptions from the underlying `insertAll` call are propagated.
    */
  def insert[F[_]: Sync](
    failedInsertProducer: Producer[F, FailedInsert],
    metrics: Metrics[F],
    toLoad: List[LoaderRow]
  )(
    mkInsert: List[LoaderRow] => F[HandledResponse]
  ): F[Unit] = {

    def go(toLoad: List[LoaderRow]): F[Unit] =
      mkInsert(toLoad).flatMap {
        case HandledResponse.RequestWasInvalid(bqe) if toLoad.size <= 1 =>
          Logger[F].warn(s"Bigquery insert failed caused by invalid request: ${bqe.getMessage}") >>
            handleFailedRows(metrics, failedInsertProducer, toLoad)
        case HandledResponse.RequestWasInvalid(_) =>
          Logger[F].info(
            "Bigquery inserts failed caused by invalid request.  Will retry the inserts in smaller batches"
          ) >>
            toLoad.traverse_ { single =>
              go(List(single))
            }
        case HandledResponse.Success(response) =>
          val partitioned = partitionRowResults(toLoad, response)
          val logF = partitioned.reasonsAndLocations.toSeq.traverse {
            case (reason, location) =>
              // We cannot log the `message` because it contains customer data.  But `reason` and `location` are safe.
              Logger[F].warn(s"Bigquery inserts failed. Reason: [$reason]. Location: [$location]")
          }

          if (partitioned.stopped.nonEmpty && partitioned.failed.nonEmpty) {
            // The request contained valid rows, but they were stopped because the request also contained invalid rows.
            // Iterate again without the invalid rows.
            logF >>
              handleFailedRows(metrics, failedInsertProducer, partitioned.failed) >>
              handleSuccessfulRows(metrics, partitioned.successful) >>
              go(partitioned.stopped)
          } else {
            // Either:
            // - All rows in this iteration were successful
            // - All rows in this iteration were invalid
            // - Some rows were stopped but there were no failures.
            // The 3rd case is unexpected and unlikely. Here we treat stopped rows as failures, to avoid any potential deadlock.
            logF >>
              handleFailedRows(metrics, failedInsertProducer, partitioned.failed ::: partitioned.stopped) >>
              handleSuccessfulRows(metrics, partitioned.successful)
          }
      }

    go(toLoad)
  }

  def logRetry[F[_]: Sync](e: Throwable, details: RetryDetails): F[Unit] = {
    val detailsMsg = if (details.givingUp) "Giving up on this insert." else s"This insert will get retried again."
    Logger[F].warn(e)(s"Exception inserting rows to Bigquery. $detailsMsg")
  }

  def mkInsert[F[_]: Async](
    good: Output.BigQuery,
    bigQuery: BigQuery,
    retryPolicy: RetryPolicy[F]
  ): List[LoaderRow] => F[HandledResponse] = { lrs =>
    val request = buildRequest(good.datasetId, good.tableId, lrs)
    retryingOnAllErrors(retryPolicy, logRetry[F]) {
      Sync[F].blocking(bigQuery.insertAll(request)).map[HandledResponse](HandledResponse.Success(_)).recover {
        case bqe: BigQueryException if causedByInvalidRequest(bqe) =>
          HandledResponse.RequestWasInvalid(bqe)
      }
    }
  }

  private def causedByInvalidRequest(bqe: BigQueryException): Boolean = {
    val reasons = bqe.getErrors.asScala.flatMap(e => Option(e.getReason))
    reasons.exists(r => r === "invalid")
  }

  def getClient[F[_]: Sync](rs: BigQueryRetrySettings, projectId: String, gcpUserAgent: GcpUserAgent): F[BigQuery] = {
    val retrySettings =
      RetrySettings
        .newBuilder()
        .setInitialRetryDelay(Duration.ofSeconds(rs.initialDelay.toLong))
        .setRetryDelayMultiplier(rs.delayMultiplier)
        .setMaxRetryDelay(Duration.ofSeconds(rs.maxDelay.toLong))
        .setTotalTimeout(Duration.ofMinutes(rs.totalTimeout.toLong))
        .build

    Sync[F].delay(
      BigQueryOptions
        .newBuilder
        .setRetrySettings(retrySettings)
        .setProjectId(projectId)
        .setHeaderProvider(createGcpUserAgentHeader(gcpUserAgent))
        .build
        .getService
    )
  }

  private case class PartitionedResult(
    failed: List[LoaderRow],
    successful: List[LoaderRow],
    stopped: List[LoaderRow],
    reasonsAndLocations: Set[(String, String)]
  )

  private def partitionRowResults(
    loaderRows: List[LoaderRow],
    response: InsertAllResponse
  ): PartitionedResult = {
    val errors = response.getInsertErrors.asScala
    loaderRows.zipWithIndex.foldRight(PartitionedResult(Nil, Nil, Nil, Set.empty)) {
      case ((row, index), acc) =>
        errors.get(index) match {
          case Some(rowErrors) if rowErrors.asScala.forall(_.getReason === "stopped") =>
            // This _valid_ row did not get inserted because of an _invalid_ row elsewhere in the batch.
            acc.copy(stopped = row :: acc.stopped)
          case Some(rowErrors) =>
            val reasonsAndLocations = rowErrors.asScala.foldLeft(acc.reasonsAndLocations) {
              case (acc, e) => acc + ((e.getReason, e.getLocation))
            }
            acc.copy(
              failed              = row :: acc.failed,
              reasonsAndLocations = reasonsAndLocations
            )
          case None =>
            acc.copy(successful = row :: acc.successful)
        }
    }
  }

  private def handleFailedRows[F[_]: Sync](
    metrics: Metrics[F],
    failedInsertProducer: Producer[F, FailedInsert],
    rows: List[LoaderRow]
  ): F[Unit] = {
    val tableRows = rows.map { lr =>
      lr.data.setFactory(GsonFactory.getDefaultInstance)
      FailedInsert(lr.data.toString)
    }

    tableRows.traverse_(fi => failedInsertProducer.produce(fi)) *> metrics.failedInsertCount(tableRows.length)
  }

  private def handleSuccessfulRows[F[_]: Sync](
    metrics: Metrics[F],
    rows: List[LoaderRow]
  ): F[Unit] =
    if (rows.nonEmpty) {
      val earliestCollectorTstamp = rows.map(_.collectorTstamp).min.getMillis
      metrics.latency(earliestCollectorTstamp) *> metrics.goodCount(rows.length)
    } else Sync[F].unit

  private def buildRequest(dataset: String, table: String, loaderRows: List[LoaderRow]) = {
    val tableRows = loaderRows.map(lr => RowToInsert.of(lr.data)).asJava
    InsertAllRequest.newBuilder(TableId.of(dataset, table), tableRows).build()
  }
}
