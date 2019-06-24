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

import cats.{Monad, Show}
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref

import fs2.{Chunk, Stream}
import fs2.concurrent.{Queue, SignallingRef}

import io.chrisdavenport.log4cats.Logger

import com.google.cloud.bigquery._

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config

import RepeaterCli.GcsPath


/**
  * Resources container, allowing to manipulate all acquired entities
  * @param bigQuery BigQuery client provided by SDK
  * @param bucket destination path
  * @param env whole configuration, extracted from config files
  * @param desperates queue of events that still could not be loaded into BQ
  * @param counter counter for batches of desperates
  * @param stop a signal to stop retreving items
  */
class Resources[F[_]](val bigQuery: BigQuery,
                      val bucket: GcsPath,
                      val env: Config.Environment,
                      val desperates: Queue[F, BadRow],
                      val counter: Ref[F, Int],
                      val stop: SignallingRef[F, Boolean],
                      val statistics: Ref[F, Resources.Statistics],
                      val bufferSize: Int,
                      val windowTime: Int,
                      val backoffTime: Int) {
  def logInserted: F[Unit] = statistics.update(s => s.copy(inserted = s.inserted + 1))
  def logAbandoned: F[Unit] = statistics.update(s => s.copy(desperates = s.desperates + 1))

  def showStats(implicit L: Logger[F], M: Monad[F]): F[Unit] =
    statistics.get.flatMap(statistics => L.info(statistics.show))
}

object Resources {

  val QueueSize = 100

  case class Statistics(inserted: Int, desperates: Int)

  object Statistics {
    val start = Statistics(0, 0)

    implicit val showRepeaterStatistics: Show[Statistics] =
      s => s"Statistics: ${s.inserted} rows inserted, ${s.desperates} rows rejected"
  }

  /** Allocate all resources for an application */
  def acquire[F[_]: ConcurrentEffect: Timer: Logger](command: RepeaterCli.ListenCommand): Resource[F, Resources[F]] = {
    val environment = for {
      transformed <- Config.transform[F](command.config).value
      env         <- Sync[F].fromEither(transformed)
      bigQuery    <- services.Database.getClient[F]
      queue       <- Queue.bounded[F, BadRow](QueueSize)
      counter     <- Ref[F].of[Int](0)
      stop        <- SignallingRef[F, Boolean](false)
      statistics  <- Ref[F].of[Statistics](Statistics.start)
      _           <- Logger[F].info(s"Initializing Repeater from ${env.config.failedInserts} to ${env.config.tableId}")
    } yield new Resources(bigQuery, command.deadEndBucket, env, queue, counter, stop, statistics, command.bufferSize, command.window, command.backoff)

    Resource.make(environment)(release)
  }

  /**
    * Try to get desperate items in queue all at once
    * Assuming that data is not sinking into `desperates` anymore (`stop.set(true)` in `release`)
    */
  def pullRemaining[F[_]: Sync](desperates: Queue[F, BadRow]): F[List[BadRow]] = {
    val last = Stream
      .repeatEval(desperates.tryDequeueChunk1(10))
      .takeWhile(_.isDefined)
      .flatMap {
        case Some(chunk) => Stream.chunk(chunk)
        case None => Stream.empty
      }
    last.compile.toList
  }

  /** Stop pulling data from Pubsub, flush despeates queue into GCS bucket */
  def release[F[_]: ConcurrentEffect: Timer: Logger](env: Resources[F]): F[Unit] = {
    val flushDesperate = for {
      desperates <- pullRemaining(env.desperates)
      chunk = Chunk(desperates: _*)
      _ <- Flow.sinkChunk(env.counter, env.bucket)(chunk)
    } yield chunk.size
    val graceful = (env.stop.set(true) *> flushDesperate).flatMap { count => Logger[F].warn(s"Terminating. Flushed $count desperates") }
    val forceful = Timer[F].sleep(5.seconds) *> Logger[F].error("Terminated without flushing after 5 seconds")
    Concurrent[F].race(graceful, forceful).void
  }
}
