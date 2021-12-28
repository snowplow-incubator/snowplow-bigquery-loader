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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.services.Storage

import cats.data.EitherT
import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.google.cloud.bigquery._
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.Logger
import org.joda.time.DateTime

import scala.concurrent.duration._

object Flow {
  sealed trait InsertStatus extends Product with Serializable
  case object Inserted extends InsertStatus // This event was inserted into BQ
  case object Retry extends InsertStatus    // This event was not inserted into BQ but Repeater will retry

  /**
    * Main sink processing data parsed from `failedInserts` topic.
    * Attempt to insert a record into BigQuery. If insertion fails, turn it into an `Uninsertable`
    * and forward to a dedicated queue, which will later be sunk to GCS.
    * @param resources All application resources
    * @param events Stream of events from Pub/Sub
    */
  def sink[F[_]: Logger: Timer: Concurrent: ContextShift](
    resources: Resources[F]
  )(events: EventStream[F]): Stream[F, Unit] =
    events.parEvalMapUnordered(resources.concurrency) { event =>
      val insert = checkAndInsert[F](
        resources.bigQuery,
        resources.env.config.output.good.datasetId,
        resources.env.config.output.good.tableId,
        resources.backoffPeriod
      )(event)
      resources.insertBlocker.blockOn(insert).flatMap {
        case Right(Inserted) => resources.metrics.repeaterInsertedCount
        case Right(_)        => Sync[F].unit
        // format: off
        case Left(d) => resources.uninsertable.enqueue1(d)
        // format: on
      }
    }

  /** Dequeue uninsertable events and sink them to GCS */
  def dequeueUninsertable[F[_]: Timer: Concurrent: Logger](
    resources: Resources[F]
  ): Stream[F, Unit] =
    resources
      .uninsertable
      .dequeueChunk(resources.bufferSize)
      .groupWithin(resources.bufferSize, resources.timeout.seconds)
      .evalMap(sinkBadChunk(resources.counter, resources.bucket, resources.metrics))

  /**
    * Sink whole chunk of uninsertable events to a file.
    * The filename is composed of time and chunk number.
    */
  def sinkBadChunk[F[_]: Timer: Sync: Logger](
    counter: Ref[F, Int],
    bucket: GcsPath,
    metrics: Metrics[F]
  )(chunk: Chunk[BadRow]): F[Unit] =
    for {
      time <- getTime
      _    <- Logger[F].debug(s"Preparing to sink chunk, $time")
      _    <- counter.update(_ + 1)
      i    <- counter.get
      file = Storage.getFileName(bucket.path, i)
      _ <- Logger[F].info(s"Filename will be $file")
      _ <- Storage.uploadChunk[F](bucket.bucket, file, chunk, metrics)
    } yield ()

  def checkAndInsert[F[_]: Sync: Logger](
    client: BigQuery,
    datasetId: String,
    tableId: String,
    backoffTime: Int
  )(event: EventRecord[F]): F[Either[BadRow, InsertStatus]] = {
    val res = for {
      ready <- EitherT.right(event.value.isReady(backoffTime.toLong))
      // We never actually generate a bad row here
      result <- EitherT[F, BadRow, InsertStatus] {
        if (ready) {
          event.ack >> EitherT(services.Database.insert[F](client, datasetId, tableId, event.value))
            .as(Inserted.asInstanceOf[InsertStatus])
            .value
        } else {
          Logger[F].debug(s"Event ${event.value.eventId}/${event.value.etlTstamp} is not ready yet. Nack") >>
            event.nack.as(Retry.asInstanceOf[InsertStatus].asRight)
        }
      }
    } yield result
    res.value
  }

  private def getTime[F[_]: Sync]: F[DateTime] =
    Sync[F].delay(DateTime.now())
}
