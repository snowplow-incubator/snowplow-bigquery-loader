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

import org.joda.time.Instant
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
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
import com.snowplowanalytics.snowplow.badrows.BadRow
import RepeaterCli.GcsPath

/**
  * Resources container, allowing to manipulate all acquired entities
  * @param bigQuery BigQuery client provided by SDK
  * @param bucket destination path
  * @param env whole configuration, extracted from config files
  * @param desperates queue of events that still could not be loaded into BQ
  * @param counter counter for batches of desperates
  * @param stop a signal to stop retreving items
  * @param concurrency a number of streams to execute inserts
  */
class Resources[F[_]: Sync](
  val bigQuery: BigQuery,
  val bucket: GcsPath,
  val env: Config.Environment,
  val desperates: Queue[F, BadRow],
  val counter: Ref[F, Int],
  val stop: SignallingRef[F, Boolean],
  val statistics: Ref[F, Resources.Statistics],
  val bufferSize: Int,
  val windowTime: Int,
  val backoffTime: Int,
  val concurrency: Int,
  val insertBlocker: Blocker,
  val jobStartTime: Instant
) {
  def logInserted: F[Unit] =
    statistics.update(s => s.copy(inserted = s.inserted + 1))
  def logAbandoned: F[Unit] =
    statistics.update(s => s.copy(desperates = s.desperates + 1))
  def updateLifetime: F[Unit] =
    for {
      instant <- Sync[F].delay(Instant.now())
      nlt <- Sync[F].delay(
        Duration(instant.getMillis - jobStartTime.getMillis, "millis")
      )
      _ <- statistics.update(s => s.copy(lifetime = nlt))
    } yield ()

  def showStats(implicit L: Logger[F], M: Monad[F]): F[Unit] =
    statistics.get.flatMap(statistics => L.info(statistics.show))
}

object Resources {

  val QueueSize = 100

  case class Statistics(inserted: Int, desperates: Int, lifetime: Duration)

  object Statistics {
    val start = Statistics(0, 0, Duration(0, "millis"))

    implicit val showRepeaterStatistics: Show[Statistics] = {
      case s if (s.lifetime.toHours == 0) =>
        s"Statistics: ${s.inserted} rows inserted, ${s.desperates} rows rejected in ${s.lifetime.toMinutes} minutes."
      case s if (s.lifetime.toHours > 24) =>
        s"Statistics: ${s.inserted} rows inserted, ${s.desperates} rows rejected in ${s.lifetime.toDays} days and ${s.lifetime.toHours - s.lifetime.toDays * 24} hours."
      case s =>
        s"Statistics: ${s.inserted} rows inserted, ${s.desperates} rows rejected in ${s.lifetime.toHours} hours."
    }
  }

  /** Allocate all resources for an application */
  def acquire[F[_]: ConcurrentEffect: Timer: Logger](
    cmd: RepeaterCli.ListenCommand
  ): Resource[F, Resources[F]] = {
    // It's a function because blocker needs to be created as Resource
    val initResources: F[Blocker => Resources[F]] = for {
      transformed <- Config.transform[F](cmd.config).value
      env         <- Sync[F].fromEither(transformed)
      bigQuery    <- services.Database.getClient[F]
      queue       <- Queue.bounded[F, BadRow](QueueSize)
      counter     <- Ref[F].of[Int](0)
      stop        <- SignallingRef[F, Boolean](false)
      statistics  <- Ref[F].of[Statistics](Statistics.start)
      concurrency <- Sync[F].delay(Runtime.getRuntime.availableProcessors * 16)
      _ <- Logger[F].info(
        s"Initializing Repeater from ${env.config.failedInserts} to ${env.config.tableId} with $concurrency streams"
      )
      jobStartTime <- Sync[F].delay(Instant.now())
    } yield (b: Blocker) =>
      new Resources(
        bigQuery,
        cmd.deadEndBucket,
        env,
        queue,
        counter,
        stop,
        statistics,
        cmd.bufferSize,
        cmd.window,
        cmd.backoff,
        concurrency,
        b,
        jobStartTime
      )

    val createBlocker = Sync[F].delay(Executors.newCachedThreadPool()).map(ExecutionContext.fromExecutorService)
    for {
      blocker   <- Resource.make(createBlocker)(ec      => Sync[F].delay(ec.shutdown())).map(Blocker.liftExecutionContext)
      resources <- Resource.make(initResources.map(init => init.apply(blocker)))(release)
    } yield resources
  }

  /**
    * Try to get desperate items in queue all at once
    * Assuming that data is not sinking into `desperates` anymore (`stop.set(true)` in `release`)
    */
  def pullRemaining[F[_]: Sync](
    desperates: Queue[F, BadRow]
  ): F[List[BadRow]] = {
    val last = Stream.repeatEval(desperates.tryDequeueChunk1(20)).takeWhile(_.isDefined).flatMap {
      case Some(chunk) => Stream.chunk(chunk)
      case None        => Stream.empty
    }
    last.compile.toList
  }

  /** Stop pulling data from Pubsub, flush despeates queue into GCS bucket */
  def release[F[_]: ConcurrentEffect: Timer: Logger](
    env: Resources[F]
  ): F[Unit] = {
    val flushDesperate = for {
      desperates <- pullRemaining(env.desperates)
      chunk = Chunk(desperates: _*)
      _ <- Flow.sinkBadChunk(env.counter, env.bucket)(chunk)
    } yield chunk.size
    val graceful = (env.stop.set(true) *> flushDesperate).flatMap { count =>
      Logger[F].warn(s"Terminating. Flushed $count desperates")
    }
    val forceful = Timer[F].sleep(5.seconds) *> Logger[F].error(
      "Terminated without flushing after 5 seconds"
    )
    Concurrent[F].race(graceful, forceful).void
  }
}
