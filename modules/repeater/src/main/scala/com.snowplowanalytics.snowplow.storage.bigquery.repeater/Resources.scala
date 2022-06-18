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
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.RepeaterEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.ReportingApp
import com.snowplowanalytics.snowplow.storage.bigquery.common.Sentry
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.{GcsPath, validateBucket}

import cats.effect._
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.cloud.bigquery._
import fs2.{Chunk, Stream}
import fs2.concurrent.{SignallingRef}
import org.typelevel.log4cats.Logger
import org.joda.time.Instant

import scala.concurrent.duration._

/**
  * @param bigQuery BigQuery client provided by SDK
  * @param bucket Destination for uninsertable events
  * @param env App configuration extracted from config files
  * @param uninsertable Queue of events that could not be loaded into BQ after multiple attempts
  * @param counter Counter for batches of uninsertable events
  * @param stop Signal to stop retrieving items
  * @param bufferSize Max size of uninsertable buffer (buffer is flushed when this is reached)
  * @param timeout Max time to wait before flushing the uninsetable buffer (if bufferSize is not reached first)
  * @param backoffPeriod Time to wait before trying to re-insert events
  * @param concurrency Number of streams to execute inserts
  */
final class Resources[F[_]](
  val bigQuery: BigQuery,
  val bucket: GcsPath,
  val env: RepeaterEnvironment,
  val uninsertable: Queue[F, BadRow],
  val counter: Ref[F, Int],
  val stop: SignallingRef[F, Boolean],
  val bufferSize: Int,
  val timeout: Int,
  val backoffPeriod: Int,
  val concurrency: Int,
  val jobStartTime: Instant,
  val metrics: Metrics[F],
  val sentry: Sentry[F]
)

object Resources {
  private val QueueSize = 100

  /** Allocate all resources */
  def acquire[F[_]: Async: Logger](
    cmd: RepeaterCli.ListenCommand
  ): Resource[F, Resources[F]] = {
    // It's a function because blocker needs to be created as Resource
    val initResources: F[(Sentry[F]) => F[Resources[F]]] = for {
      env      <- Sync[F].delay(cmd.env)
      bigQuery <- services.Database.getClient[F](cmd.env.projectId)
      bucket <- Sync[F].fromEither(
        validateBucket(env.config.output.deadLetters.bucket)
          .toEither
          .leftMap(e => new RuntimeException(e.toList.mkString("\n")))
      )
      queue       <- Queue.bounded[F, BadRow](QueueSize)
      counter     <- Ref[F].of[Int](0)
      stop        <- SignallingRef[F, Boolean](false)
      concurrency <- Sync[F].delay(Runtime.getRuntime.availableProcessors * 16)
      _ <- Logger[F].info(
        s"Initializing Repeater from ${env.config.input.subscription} to ${env.config.output.good} with $concurrency streams"
      )
      jobStartTime <- Sync[F].delay(Instant.now())
    } yield (sentry: Sentry[F]) =>
      mkMetricsReporter[F](env.monitoring).map(m =>
        new Resources[F](
          bigQuery,
          bucket,
          env,
          queue,
          counter,
          stop,
          cmd.bufferSize,
          cmd.timeout,
          cmd.backoffPeriod,
          concurrency,
          jobStartTime,
          m,
          sentry
        )
      )
    for {
      sentry    <- Sentry.init(cmd.env.monitoring.sentry)
      resources <- Resource.make(initResources.flatMap(init => init.apply(sentry)))(release[F])
    } yield resources
  }

  def mkMetricsReporter[F[_]: Async: Logger](
    monitoringConfig: Monitoring
  ): F[Metrics[F]] =
    Metrics.build[F](monitoringConfig, ReportingApp.Repeater)

  /**
    * Try to get uninsertable events into the queue all at once,
    * assuming that data is not sinking into `uninsertable` anymore (`stop.set(true)` in `release`).
    */
  def pullRemaining[F[_]: Sync](
    uninsertable: Queue[F, BadRow]
  ): F[List[BadRow]] = {
    val last = Stream.repeatEval(uninsertable.tryTake).takeWhile(_.isDefined).flatMap {
      case Some(item) => Stream.emit(item)
      case None       => Stream.empty
    }
    last.compile.toList
  }

  /** Stop pulling data from Pub/Sub and flush uninsertable queue into GCS bucket */
  def release[F[_]: Async: Logger](res: Resources[F]): F[Unit] = {
    val flushUninsertable = for {
      uninsertable <- pullRemaining[F](res.uninsertable)
      chunk = Chunk.seq(uninsertable)
      _ <- Flow.sinkBadChunk(res.counter, res.bucket, res.metrics)(chunk)
    } yield chunk.size

    val graceful =
      (res.stop.set(true) *> flushUninsertable).flatMap { count =>
        Logger[F].warn(s"Terminating. Flushed $count events that could not be inserted into BigQuery")
      }

    val forceful = Async[F].sleep(5.seconds) *> Logger[F].error(
      "Terminated without flushing after 5 seconds"
    )
    Concurrent[F].race(graceful, forceful).void
  }
}
