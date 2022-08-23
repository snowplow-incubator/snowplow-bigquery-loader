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

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Async, Deferred, Sync}
import cats.effect.Resource.ExitCase
import cats.effect.std.Queue

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.storage.bigquery.common.Sentry

import scala.concurrent.duration.FiniteDuration

object Shutdown {

  implicit private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
    * This is the machinery needed to make sure outstanding records are acked before the app
    * terminates
    *
    * The `source` and `sink` each run in concurrent streams, so they are not immediately cancelled
    * upon receiving a SIGINT
    *
    * The "main" stream just waits for a SIGINT, and then cleanly shuts down the concurrent processes.
    *
    * We use a queue as a level of indirection between the stream of events and the sink + acking.
    * When we receive a SIGINT or exception then we terminate the sink by pushing a `None` to the
    * queue.
    */
  def run[F[_]: Async, A](
    timeout: FiniteDuration,
    sentry: Sentry[F],
    source: Stream[F, A],
    sinkAndAck: Pipe[F, A, Unit]
  ): Stream[F, Unit] =
    for {
      queue <- Stream.eval(Queue.synchronous[F, Option[A]])
      sig   <- Stream.eval(Deferred[F, Unit])
      _     <- impl(timeout, sentry, source, sinkAndAck, queue, sig)
    } yield ()

  private def impl[F[_]: Async, A](
    timeout: FiniteDuration,
    sentry: Sentry[F],
    source: Stream[F, A],
    sinkAndAck: Pipe[F, A, Unit],
    queue: Queue[F, Option[A]],
    sig: Deferred[F, Unit]
  ): Stream[F, Unit] =
    Stream
      .eval(sig.get)
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          // Both the source and sink have completed "naturally".
          Sync[F].unit
        case ExitCase.Canceled =>
          // SIGINT received. We wait for events already in the queue to get sunk and acked
          terminateStream(queue, sig, timeout)
        case ExitCase.Errored(e) =>
          // Runtime exception either in the source or in the sink.
          // The exception is already logged by the concurrent process.
          // We wait for the transformed events already in the queue to get sunk and acked.
          // We then raise the original exception
          terminateStream(queue, sig, timeout).handleErrorWith { e2 =>
            Logger[F].error(e2)("Error when terminating the sink and checkpoint")
          } *> Sync[F].raiseError[Unit](e)
      }
      .concurrently {
        Stream.bracket(().pure[F])(_ => sig.complete(()).void) >>
          Stream.fromQueueNoneTerminated(queue).through(sinkAndAck).onFinalizeCase {
            case ExitCase.Succeeded =>
              // The queue has completed "naturally", i.e. a `None` got enqueued.
              Logger[F].info("Completed sinking and checkpointing events")
            case ExitCase.Canceled =>
              Logger[F].info("Sinking and checkpointing was cancelled")
            case ExitCase.Errored(e) =>
              sentry.trackException(e) *>
                Logger[F].error(e)("Error on sinking and checkpointing events")
          }
      }
      .concurrently {
        source.evalMap(x => queue.offer(Some(x))).onFinalizeCase {
          case ExitCase.Succeeded =>
            // The source has completed "naturally".
            Logger[F].info("Reached the end of the source of events") *>
              closeQueue(queue)
          case ExitCase.Canceled =>
            Logger[F].info("Source of events was cancelled")
          case ExitCase.Errored(e) =>
            sentry.trackException(e) *>
              Logger[F].error(e)("Error in the source of events") *>
              terminateStream(queue, sig, timeout)
        }
      }

  // Closing the queue allows the sink to finish once all pending events have been flushed to BigQuery.
  // We spawn it as a Fiber because `queue.offer(None)` can hang if the queue is already closed.
  private def closeQueue[F[_]: Async, A](queue: Queue[F, Option[A]]): F[Unit] =
    queue.offer(None).start.void

  private def terminateStream[F[_]: Async, A](
    queue: Queue[F, Option[A]],
    sig: Deferred[F, Unit],
    timeout: FiniteDuration
  ): F[Unit] =
    for {
      _ <- Logger[F].warn(s"Terminating event sink. Waiting $timeout for it to complete")
      _ <- closeQueue(queue)
      _ <- sig.get.timeoutTo(timeout, Logger[F].warn(s"Sink not complete after $timeout, aborting"))
    } yield ()
}
