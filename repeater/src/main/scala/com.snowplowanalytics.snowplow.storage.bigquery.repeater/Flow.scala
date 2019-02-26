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

import org.joda.time.DateTime

import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref

import fs2.{ Stream, Chunk }
import scala.concurrent.duration._


import io.chrisdavenport.log4cats.Logger

import com.google.cloud.bigquery._

import com.permutive.pubsub.consumer.Model

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.services.Storage


object Flow {

  /**
    * Main sink, processing data parsed from `failedInserts`
    * Attempts to insert a record into BigQuery. If insertion fails - turn it into a `Desperate`
    * and forward to a specific queue, which later will be sinked into GCS
    * @param resources all application resources
    * @param events stream of events from PubSub
    */
  def process[F[_]: Logger: Timer: ConcurrentEffect](resources: Resources[F])(events: Stream[F, Model.Record[F, EventContainer]]): Stream[F, Unit] = {
    val inserting = events
      .evalMap(checkAndInsert[F](resources.bigQuery, resources.env.config.datasetId, resources.env.config.tableId, resources.backoffTime))
      .evalMap {
        case Right(_) => resources.logInserted
        case Left(d) => resources.logAbandoned *> resources.desperates.enqueue1(d)
      }
    inserting.concurrently(dequeueDesperates(resources))
  }

  /** Process dequeueing desperates and sinking them to GCS */
  def dequeueDesperates[F[_]: Timer: Concurrent: Logger](resources: Resources[F]): Stream[F, Unit] =
    resources
      .desperates
      .dequeueChunk(resources.bufferSize)
      .groupWithin(resources.bufferSize, resources.windowTime.seconds)
      .evalMap(sinkChunk(resources.counter, resources.bucket))

  /** Sink whole chunk of desperates into a filename, composed of time and chunk number */
  def sinkChunk[F[_]: Timer: Sync: Logger](counter: Ref[F, Int], bucket: GcsPath)(chunk: Chunk[Desperate]): F[Unit] =
    for {
      time <- getTime
      _ <- Logger[F].debug(s"Preparing for sinking a chunk, $time")
      _ <- counter.update(_ + 1)
      i <- counter.get
      file = Storage.getFileName(bucket.path, i, time)
      _ <- Logger[F].debug(s"Filename will be $file")
      _  <- Storage.uploadChunk[F](bucket.bucket, file, chunk)
    } yield ()

  def checkAndInsert[F[_]: Sync: Logger](client: BigQuery,
                                         dataset: String,
                                         table: String,
                                         backoffTime: Int)
                                        (event: Model.Record[F, EventContainer]): F[Either[Desperate, Unit]] =
    for {
      ready <- event.value.isReady(backoffTime)
      result <- if (ready)
        event.ack >>
          services.Database.insert[F](client, dataset, table, event.value)
      else
        Logger[F].debug(s"Event ${event.value.eventId}/${event.value.etlTstamp} is not ready yet. Nack") >>
          event.nack.map(_.asRight)
    } yield result

  private def getTime[F[_]: Sync]: F[DateTime] =
    Sync[F].delay(DateTime.now())
}