/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader

import cats.syntax.all._
import cats.effect.{Concurrent, ContextShift, ExitCode, IO, Timer}

import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.events._
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub.{WriteBadRow, WriteObservedTypes}
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow

import org.slf4j.LoggerFactory

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType

object Fs2Loader {
  private val MaxConcurrency = 5

  def goodSink(typesQueue: Queue[IO, Set[ShreddedType]])(loaderRows: Stream[IO, LoaderRow])(implicit C: Concurrent[IO], T: Timer[IO]): Stream[IO, Unit] = {
    loaderRows.through(enqueueTypes[LoaderRow, ShreddedType](typesQueue, _.inventory)) *> bigquerySink(loaderRows)
  }

  // TODO: Add non-static BigQuery Sink
  def bigquerySink(loaderRows: Stream[IO, LoaderRow])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    loaderRows.parEvalMapUnordered(MaxConcurrency) { loaderRow =>
      log(loaderRow.toString)("good")
    }

  import concurrent.duration._

  def aggregateTypes[A](implicit T: Timer[IO], C: Concurrent[IO]): Pipe[IO, Set[A], Set[A]] =
    types => types.groupWithin(3, 2.seconds).map { chunk => chunk.toList.toSet.flatten }

  def enqueueTypes[R, T](queue: Queue[IO, Set[T]], f: R => Set[T])(implicit C: Concurrent[IO], T: Timer[IO]): Pipe[IO, R, Unit] =
    _.map(f).through[IO, Set[T]](aggregateTypes[T](T, C)).through(queue.enqueue)

  def badSink(env: Environment, static: Boolean)(badRows: Stream[IO, BadRow])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    if (static) {
      badRows.parEvalMapUnordered(MaxConcurrency) { badRow =>
        log(badRow.toString)("bad")
      }
    } else {
      badRows.parEvalMapUnordered(MaxConcurrency) { badRow =>
        PubSub.sink(env.config.projectId, env.config.badRows)(WriteBadRow(badRow))
      }
    }

  def dequeueTypes(env: Environment, static: Boolean)(typesQueue: Queue[IO, Set[ShreddedType]])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    typesQueue.dequeue.parEvalMapUnordered(MaxConcurrency) { typesSet =>
      if (static) {
        log(toPayload(typesSet).noSpaces)("types")
      } else {
        PubSub.sink(env.config.projectId, env.config.typesTopic)(WriteObservedTypes(typesSet))
      }
    }

  def run(env: Environment)(static: Boolean = true)(implicit cs: ContextShift[IO], c: Concurrent[IO], T: Timer[IO]): IO[ExitCode] = {
    val source = {
      if (static) {
        new StaticSource(10)
      } else {
        new PubsubSource(env)(cs, c)
      }
    }

    val eventStream: Stream[IO, Either[BadRow, LoaderRow]] =
      source.getStream.map(LoaderRow.parse(env.resolverJson))

    for {
      queue <- Queue.bounded[IO, Set[ShreddedType]](MaxConcurrency)
      sinkBadGood = eventStream.observeEither[BadRow, LoaderRow](badSink(env, static), goodSink(queue))
      sinkTypes = dequeueTypes(env, static)(queue)
      _ <- sinkBadGood.compile.drain *> sinkTypes.compile.drain
    } yield ExitCode.Success
  }

  private def log(s: String)(name: String): IO[Unit] = IO.delay(LoggerFactory.getLogger(name).info(s))

}
