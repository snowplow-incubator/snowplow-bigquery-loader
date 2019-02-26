/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import cats.syntax.all._
import cats.effect.Sync

import com.google.cloud.bigquery._

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer

import io.chrisdavenport.log4cats.Logger

/** Module responsible for communication with BigQuery */
object Database {

  /** Insert an enriched event, parsed from `failedInserts` into BigQuery */
  def insert[F[_]: Sync: Logger](client: BigQuery, dataset: String, table: String, event: EventContainer): F[Either[Desperate, Unit]] = {
    val request = buildRequest(dataset, table, event)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) if response.hasErrors =>
        EventContainer.Desperate(event.payload, EventContainer.FailedRetry.extract(response.getInsertErrors)).asLeft
      case Right(_) =>
        ().asRight
      case Left(throwable: BigQueryException) =>
        Desperate(event.payload, EventContainer.FailedRetry.extract(throwable)).asLeft
      case Left(unknown) =>
        throw unknown
    }.flatTap {
      case Right(_) =>
        Logger[F].debug(s"Event ${event.eventId}/${event.etlTstamp} successfully inserted")
      case Left(desperate) =>
        Logger[F].debug(s"Event ${event.eventId}/${event.etlTstamp} could not be inserted. ${desperate.error}")
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, event: EventContainer) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table))
      .addRow(event.eventId.toString, event.decompose)
      .build()
}
