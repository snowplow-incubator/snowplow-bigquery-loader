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

import scala.concurrent.duration._

import org.joda.time.DateTime

import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.data.EitherT

import fs2.{Chunk, Stream}

import io.chrisdavenport.log4cats.Logger

import com.google.cloud.bigquery._

import com.snowplowanalytics.snowplow.badrows.BadRow

import RepeaterCli.GcsPath
import services.Storage

object Flow {

  /**
    * Main sink, processing data parsed from `failedInserts`
    * Attempts to insert a record into BigQuery. If insertion fails - turn it into a `Desperate`
    * and forward to a specific queue, which later will be sinked into GCS
    * @param resources all application resources
    * @param events stream of events from PubSub
    */
  def sink[F[_]: Logger: Timer: Concurrent: ContextShift](
    resources: Resources[F]
  )(events: EventStream[F]): Stream[F, Unit] =
    events.parEvalMapUnordered(resources.concurrency) { event =>
      val insert = checkAndInsert[F](
        resources.bigQuery,
        resources.env.config.datasetId,
        resources.env.config.tableId,
        resources.backoffTime
      )(event)
      resources.insertBlocker.blockOn(insert).flatMap {
        case Right(_) =>
          resources.updateLifetime
          resources.logInserted
        case Left(d) =>
          resources.updateLifetime
          resources.logAbandoned *> resources.desperates.enqueue1(d)
      }
    }

  /** Process dequeueing desperates and sinking them to GCS */
  def dequeueDesperates[F[_]: Timer: Concurrent: Logger](
    resources: Resources[F]
  ): Stream[F, Unit] =
    resources.desperates
      .dequeueChunk(resources.bufferSize)
      .groupWithin(resources.bufferSize, resources.windowTime.seconds)
      .evalMap(sinkBadChunk(resources.counter, resources.bucket))

  /** Sink whole chunk of desperates into a filename, composed of time and chunk number */
  def sinkBadChunk[F[_]: Timer: Sync: Logger](
    counter: Ref[F, Int],
    bucket: GcsPath
  )(chunk: Chunk[BadRow]): F[Unit] =
    for {
      time <- getTime
      _ <- Logger[F].debug(s"Preparing for sinking a chunk, $time")
      _ <- counter.update(_ + 1)
      i <- counter.get
      file = Storage.getFileName(bucket.path, i, time)
      _ <- Logger[F].debug(s"Filename will be $file")
      _ <- Storage.uploadChunk[F](bucket.bucket, file, chunk)
    } yield ()

  def checkAndInsert[F[_]: Sync: Logger](
    client: BigQuery,
    dataset: String,
    table: String,
    backoffTime: Int
  )(event: EventRecord[F]): F[Either[BadRow, Unit]] = {
    val res = for {
      ready <- EitherT.right(event.value.isReady(backoffTime))
      result <- EitherT[F, BadRow, Unit] {
        if (ready)
          event.ack >>
            EitherT(
              services.Database.insert[F](client, dataset, table, event.value)
            ).value
        else
          Logger[F].debug(
            s"Event ${event.value.eventId}/${event.value.etlTstamp} is not ready yet. Nack"
          ) >>
            event.nack.map(_.asRight)
      }
    } yield result
    res.value
  }

  private def getTime[F[_]: Sync]: F[DateTime] =
    Sync[F].delay(DateTime.now())
}
