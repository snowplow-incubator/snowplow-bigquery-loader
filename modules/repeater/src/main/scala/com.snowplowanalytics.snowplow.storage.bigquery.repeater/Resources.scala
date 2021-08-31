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
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.RepeaterEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.ReportingApp
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.{GcsPath, validateBucket}

import cats.Show
import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.google.cloud.bigquery._
import fs2.{Chunk, Stream}
import fs2.concurrent.{Queue, SignallingRef}
import io.chrisdavenport.log4cats.Logger
import org.joda.time.Instant

import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

/**
  * @param bigQuery BigQuery client provided by SDK
  * @param bucket Destination for uninsertable events
  * @param env App configuration extracted from config files
  * @param uninsertable Queue of events that could not be loaded into BQ after multiple attempts
  * @param counter Counter for batches of uninsertable events
  * @param stop Signal to stop retrieving items
  * @param statistics Holds information about inserted and uninsertable events over time
  * @param bufferSize Max size of uninsertable buffer (buffer is flushed when this is reached)
  * @param timeout Max time to wait before flushing the uninsetable buffer (if bufferSize is not reached first)
  * @param backoffPeriod Time to wait before trying to re-insert events
  * @param concurrency Number of streams to execute inserts
  */
final class Resources[F[_]: Sync](
  val bigQuery: BigQuery,
  val bucket: GcsPath,
  val env: RepeaterEnvironment,
  val uninsertable: Queue[F, BadRow],
  val counter: Ref[F, Int],
  val stop: SignallingRef[F, Boolean],
  val statistics: Ref[F, Resources.Statistics],
  val bufferSize: Int,
  val timeout: Int,
  val backoffPeriod: Int,
  val concurrency: Int,
  val insertBlocker: Blocker,
  val jobStartTime: Instant,
  val metrics: Metrics[F]
) {
  def logInserted: F[Unit] =
    statistics.update(s => s.copy(inserted = s.inserted + 1))
  def logAbandoned: F[Unit] =
    statistics.update(s => s.copy(uninsertable = s.uninsertable + 1))
  def logReinsert: F[Unit] =
    statistics.update(s => s.copy(reinsert = s.reinsert + 1))
  def updateLifetime: F[Unit] =
    for {
      now <- Sync[F].delay(Instant.now())
      newLifetime = Duration(now.getMillis - jobStartTime.getMillis, "millis")
      _ <- statistics.update(s => s.copy(lifetime = newLifetime))
    } yield ()
  def showStats(implicit L: Logger[F]): F[Unit] =
    statistics.get.flatMap(statistics => L.info(statistics.show))
}

object Resources {
  private val QueueSize = 100

  final case class Statistics(inserted: Int, uninsertable: Int, reinsert: Int, lifetime: Duration)
  object Statistics {
    val start: Statistics = Statistics(0, 0, 0, Duration(0, "millis"))

    implicit val showRepeaterStatistics: Show[Statistics] = {
      case s if (s.lifetime.toHours == 0) =>
        s"${common(s)} ${s.lifetime.toMinutes} minutes."
      case s if (s.lifetime.toHours > 24) =>
        s"${common(s)} ${s.lifetime.toDays} days and ${s.lifetime.toHours - s.lifetime.toDays * 24} hours."
      case s =>
        s"${common(s)} ${s.lifetime.toHours} hours."
    }

    private def common(s: Statistics): String =
      s"Statistics: ${s.reinsert} rows nack'ed for reinsertion, ${s.inserted} rows inserted, ${s.uninsertable} rows rejected in"
  }

  /** Allocate all resources */
  def acquire[F[_]: ConcurrentEffect: ContextShift: Timer: Logger](
    cmd: RepeaterCli.ListenCommand
  ): Resource[F, Resources[F]] = {
    // It's a function because blocker needs to be created as Resource
    val initResources: F[Blocker => F[Resources[F]]] = for {
      env      <- Sync[F].delay(cmd.env)
      bigQuery <- services.Database.getClient[F]
      bucket <- Sync[F].fromEither(
        validateBucket(env.config.output.deadLetters.bucket)
          .toEither
          .leftMap(e => new RuntimeException(e.toList.mkString("\n")))
      )
      queue       <- Queue.bounded[F, BadRow](QueueSize)
      counter     <- Ref[F].of[Int](0)
      stop        <- SignallingRef[F, Boolean](false)
      statistics  <- Ref[F].of[Statistics](Statistics.start)
      concurrency <- Sync[F].delay(Runtime.getRuntime.availableProcessors * 16)
      _ <- Logger[F].info(
        s"Initializing Repeater from ${env.config.input.subscription} to ${env.config.output.good} with $concurrency streams"
      )
      jobStartTime <- Sync[F].delay(Instant.now())
    } yield (b: Blocker) =>
      mkMetricsReporter[F](b, env.monitoring).map(m =>
        new Resources[F](
          bigQuery,
          bucket,
          env,
          queue,
          counter,
          stop,
          statistics,
          cmd.bufferSize,
          cmd.timeout,
          cmd.backoffPeriod,
          concurrency,
          b,
          jobStartTime,
          m
        )
      )

    val createBlocker = Sync[F].delay(Executors.newCachedThreadPool()).map(ExecutionContext.fromExecutorService)
    for {
      blocker   <- Resource.make(createBlocker)(ec => Sync[F].delay(ec.shutdown())).map(Blocker.liftExecutionContext)
      resources <- Resource.make(initResources.flatMap(init => init.apply(blocker)))(release[F])
    } yield resources
  }

  def mkMetricsReporter[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    monitoringConfig: Monitoring
  ): F[Metrics[F]] =
    Metrics.build[F](blocker, monitoringConfig.statsd, ReportingApp.Repeater)

  /**
    * Try to get uninsertable events into the queue all at once,
    * assuming that data is not sinking into `uninsertable` anymore (`stop.set(true)` in `release`).
    */
  def pullRemaining[F[_]: Sync](
    uninsertable: Queue[F, BadRow]
  ): F[List[BadRow]] = {
    val last = Stream.repeatEval(uninsertable.tryDequeueChunk1(20)).takeWhile(_.isDefined).flatMap {
      case Some(chunk) => Stream.chunk(chunk)
      case None        => Stream.empty
    }
    last.compile.toList
  }

  /** Stop pulling data from Pub/Sub and flush uninsertable queue into GCS bucket */
  def release[F[_]: ConcurrentEffect: Timer: Logger](res: Resources[F]): F[Unit] = {
    val flushUninsertable = for {
      uninsertable <- pullRemaining[F](res.uninsertable)
      chunk = Chunk.seq(uninsertable)
      _ <- Flow.sinkBadChunk(res.counter, res.bucket, res.metrics)(chunk)
    } yield chunk.size

    val graceful =
      (res.stop.set(true) *> flushUninsertable).flatMap { count =>
        Logger[F].warn(s"Terminating. Flushed $count events that could not be inserted into BigQuery")
      }

    val forceful = Timer[F].sleep(5.seconds) *> Logger[F].error(
      "Terminated without flushing after 5 seconds"
    )
    Concurrent[F].race(graceful, forceful).void
  }
}
