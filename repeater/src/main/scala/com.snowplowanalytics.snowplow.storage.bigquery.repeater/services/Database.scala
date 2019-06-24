package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import cats.syntax.all._
import cats.effect.Sync

import com.google.cloud.bigquery._

import io.chrisdavenport.log4cats.Logger

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.BadRow._
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{BadRow, EventContainer}

/** Module responsible for communication with BigQuery */
object Database {

  /** Insert an enriched event, parsed from `failedInserts` into BigQuery */
  def insert[F[_]: Sync: Logger](client: BigQuery, dataset: String, table: String, eventContainer: EventContainer): F[Either[InternalErrorInfo, Unit]] = {
    val request = buildRequest(dataset, table, eventContainer)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) if response.hasErrors =>
        BadRow.extract(response.getInsertErrors).asLeft
      case Right(_) =>
        ().asRight
      case Left(throwable: BigQueryException) =>
        BadRow.extract(throwable).asLeft
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
}
