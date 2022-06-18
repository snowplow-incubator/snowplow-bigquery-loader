/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.storage.bigquery.common.{LoaderRow, Sentry}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment

import cats.Monad
import cats.effect._
import cats.effect.std.Queue
import cats.effect.implicits._
import cats.implicits._

import fs2.{Pipe, Stream}

import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

object StreamLoader {
  private val processor: Processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  /**
    * PubSub message with successfully parsed row, ready to be inserted into BQ.
    * Includes an `ack` action to be performed after the event is sunk.
    */
  final case class StreamLoaderRow[F[_]](row: LoaderRow, ack: F[Unit])

  /**
    * PubSub message with a row that failed parsing (a `BadRow`).
    * Includes an `ack` action to be performed after the bad row is sunk.
    */
  final case class StreamBadRow[F[_]](row: BadRow, ack: F[Unit])

  type Parsed[F[_]] = Either[StreamBadRow[F], StreamLoaderRow[F]]

  def run[F[_]: Async: Logger](e: LoaderEnvironment): F[ExitCode] =
    Resources.acquire(e).use { resources =>
      val eventStream = resources.source.evalMap(parse(resources.igluClient.resolver))

      val sink: Pipe[F, Parsed[F], Nothing] = _.observeEither(
        resources.badSink,
        resources.goodSink
      ).drain

      val metrics = resources.metrics.report

      Logger[F].info(
        s"BQ Stream Loader ${generated.BuildInfo.version} has started. Listening ${e.config.input.subscription}"
      ) *>
        runWithShutdown(e.config.terminationTimeout, resources.sentry, eventStream.concurrently(metrics), sink)
          .attempt
          .flatMap {
            case Right(_) =>
              Logger[F].info("Application shutting down") >> ExitCode.Success.pure[F]
            case Left(e) =>
              Logger[F].error(e)(s"Application shutting down with error") *>
                ExitCode.Error.pure[F]
          }
    }

  /** Parse a PubSub message into a `LoaderRow` (or `BadRow`) and attach `ack` action to be used after sink. */
  def parse[F[_]: Clock: Monad: RegistryLookup](igluClient: Resolver[F])(payload: Payload[F]): F[Parsed[F]] =
    LoaderRow.parse[F](igluClient, processor)(payload.value).map {
      case Right(row) => StreamLoaderRow[F](row, payload.ack).asRight[StreamBadRow[F]]
      case Left(row)  => StreamBadRow[F](row, payload.ack).asLeft[StreamLoaderRow[F]]
    }

  /**
    * This is the machinery needed to make sure outstanding records are acked before the app
    * terminates
    *
    * The stream runs on a separate fiber so that we can manually handle SIGINT.
    *
    * We use a queue as a level of indirection between the soure and the sink. When we receive a
    * SIGINT or exception then we terminate the fiber by pushing a `None` to the queue.
    *
    * The source is only cancelled after the sink has been allowed to finish cleanly. We must not
    * terminate the source any earlier, because this would shutdown the PubSub consumer too early,
    * and then we would not be able to ack any outstanding records.
    */
  private def runWithShutdown[F[_]: Async: Logger, A](
    timeout: FiniteDuration,
    sentry: Sentry[F],
    source: Stream[F, A],
    sink: Pipe[F, A, Unit]
  ): F[Unit] =
    Queue.synchronous[F, Option[A]].flatMap { queue =>
      Stream
        .fromQueueNoneTerminated(queue)
        .through(sink)
        .concurrently(source.evalMap(x => queue.offer(Some(x))).onFinalize(queue.offer(None)))
        .compile
        .drain
        .start
        .bracketCase(_.joinWithNever) {
          case (_, Outcome.Succeeded(_)) =>
            // The source has completed "naturally".
            // For a infinite source like PubSub this should never happen.
            Async[F].unit
          case (fiber, Outcome.Canceled()) =>
            // We received a SIGINT.  We want to ack outstanding events before letting the app exit.
            Logger[F].warn("Received a shutdown signal") *>
              terminateStream(queue, fiber, timeout)
          case (fiber, Outcome.Errored(e)) =>
            // The source had a runtime exception.  We want to ack oustanding events, and raise the original exception.
            Logger[F].error(e)("Exception caused loader to terminate") *>
              sentry.trackException(e) *>
              terminateStream(queue, fiber, timeout).handleErrorWith { e2 =>
                Logger[F].error(e2)("Caught exception shutting down the stream")
              } *> Concurrent[F].raiseError(e)
        }
    }

  private def terminateStream[F[_]: Async: Logger, A](
    queue: Queue[F, Option[A]],
    fiber: Fiber[F, Throwable, Unit],
    timeout: FiniteDuration
  ): F[Unit] =
    for {
      _ <- Logger[F].warn(s"Terminating the stream. Waiting for $timeout for it to complete")
      _ <- queue.offer(None)
      _ <- fiber
        .join
        .timeoutTo(
          timeout,
          Logger[F].warn("Aborted waiting for stream to complete") *> fiber.cancel.as(Outcome.Succeeded(Async[F].unit))
        )
    } yield ()
}
