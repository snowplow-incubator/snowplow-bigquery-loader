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

import cats.effect.{Concurrent, ContextShift, ExitCode, IO}
import cats.syntax.all._
import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import fs2.{Chunk, Stream}
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.events._
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub.{WriteBadRow, WriteObservedTypes}
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow
import fs2.concurrent.Queue
import org.slf4j.LoggerFactory

object Fs2Loader {
  private val MaxConcurrency = 5

  def log(s: String)(name: String): IO[Unit] = IO.delay(LoggerFactory.getLogger(name).info(s))


  def goodSink(typesQueue: Queue[IO, Set[ShreddedType]])(loaderRows: Stream[IO, LoaderRow])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    enqueueTypes(loaderRows, typesQueue) *> bigquerySink(loaderRows)

  // TODO: Add non-static BigQuery Sink
  def bigquerySink(loaderRows: Stream[IO, LoaderRow])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    loaderRows.parEvalMapUnordered(MaxConcurrency) { loaderRow =>
      log(loaderRow.toString)("good")
    }

  def aggregateTypes(types: Stream[IO, Set[ShreddedType]]): Stream[IO, Set[ShreddedType]] =
    types.mapChunks(c => Chunk(c.foldLeft[Set[ShreddedType]](Set.empty[ShreddedType])(_ ++ _)))


  def enqueueTypes(loaderRows: Stream[IO, LoaderRow], queue: Queue[IO, Set[ShreddedType]])(implicit C: Concurrent[IO]): Stream[IO, Unit] =
    aggregateTypes(loaderRows.map(_.inventory)).parEvalMapUnordered(MaxConcurrency) { t =>
      queue.enqueue1(t)
    }

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


  def run(env: Environment)(static: Boolean = true)(implicit cs: ContextShift[IO], c: Concurrent[IO]): IO[ExitCode] = {
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
}
