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

import scala.collection.JavaConverters._

import cats.syntax.all._
import cats.effect.Sync

import io.circe.syntax._

import com.google.cloud.bigquery.{ Option => _, _ }

import io.chrisdavenport.log4cats.Logger

import com.snowplowanalytics.snowplow.badrows.{ BadRow, Failure, FailureDetails, Payload }

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{ EventContainer, Repeater }

/** Module responsible for communication with BigQuery */
object Database {

  /** Insert an enriched event, parsed from `failedInserts` into BigQuery */
  def insert[F[_]: Sync: Logger](client: BigQuery, dataset: String, table: String, eventContainer: EventContainer): F[Either[BadRow, Unit]] = {
    val request = buildRequest(dataset, table, eventContainer)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) if response.hasErrors =>
        val payload = Payload.RawPayload(eventContainer.asJson.noSpaces)
        val errors = response
          .getInsertErrors
          .asScala
          .toList
        val failure = errors
          .flatMap(_._2.asScala.toList)
          .headOption
          .map(recoveryRuntimeFailure)
          .getOrElse(Failure.LoaderRecoveryFailure(FailureDetails.LoaderRecoveryError.RuntimeError(errors.toString(), None, None)))
        (BadRow.LoaderRecoveryError(Repeater.processor, failure, payload): BadRow).asLeft
      case Right(_) =>
        ().asRight
      case Left(throwable: BigQueryException) =>
        val payload = Payload.RawPayload(eventContainer.asJson.noSpaces)
        val failure = Option(throwable.getError) match {
          case Some(e) =>
            recoveryRuntimeFailure(e)
          case None =>
            val error = FailureDetails.LoaderRecoveryError.RuntimeError(throwable.getMessage, None, None)
            Failure.LoaderRecoveryFailure(error)
        }
        BadRow.LoaderRecoveryError(Repeater.processor, failure, payload).asLeft
      case Left(unknown) =>
        throw unknown
    }.flatTap {
      case Right(_) =>
        Logger[F].debug(s"Event ${eventContainer.eventId}/${eventContainer.etlTstamp} successfully inserted")
      case Left(desperate) =>
        Logger[F].debug(s"Event ${eventContainer.eventId}/${eventContainer.etlTstamp} could not be inserted. $desperate")
    }
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  private def buildRequest(dataset: String, table: String, event: EventContainer) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table))
      .addRow(event.eventId.toString, event.decompose)
      .build()

  private def recoveryRuntimeFailure(e: BigQueryError) = {
    val info = FailureDetails.LoaderRecoveryError.RuntimeError(e.getMessage, Option(e.getLocation), Option(e.getReason))
    Failure.LoaderRecoveryFailure(info)
  }

}
