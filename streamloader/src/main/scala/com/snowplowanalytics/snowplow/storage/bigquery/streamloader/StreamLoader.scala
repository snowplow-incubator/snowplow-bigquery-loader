/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration._

import cats.implicits._
import cats.effect.{Clock, Concurrent, ContextShift, IO, Timer}

import fs2.Pipe
import fs2.concurrent.Queue

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment

object StreamLoader {

  val processor: Processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  /**
    * PubSub message with successfully parsed row, ready to be inserted into BQ.
    * Includes an `ack` action to be performed after the event is sunk.
    */
  case class StreamLoaderRow[F[_]](row: LoaderRow, ack: F[Unit])

  /**
    * PubSub message with a row that failed parsing (a `BadRow`).
    * Includes an `ack` action to be performed after the bad row is sunk.
    */
  case class StreamBadRow[F[_]](row: BadRow, ack: F[Unit])

  type Parsed = Either[StreamBadRow[IO], StreamLoaderRow[IO]]

  private val MaxConcurrency = Runtime.getRuntime.availableProcessors * 16
  private val GroupByN       = 10
  private val TimeWindow     = 30.seconds
  private val QueueSize      = 1024

  def run(e: Environment)(implicit CS: ContextShift[IO], C: Concurrent[IO], T: Timer[IO]): IO[Unit] =
    Resources.acquire(e).use { resources =>
      val eventStream = resources.source.evalMap(parse(resources.igluClient.resolver))

      for {
        queue <- Queue.bounded[IO, Set[ShreddedType]](QueueSize)
        sinkBadGood = eventStream
          .observeEither[StreamBadRow[IO], StreamLoaderRow[IO]](
            resources.badRowsSink,
            goodSink(resources, queue)
          )
          .void
        sinkTypes = queue.dequeue.through(aggregateTypes(GroupByN, TimeWindow)).through(resources.typesSink)
        _ <- sinkBadGood.merge(sinkTypes).compile.drain
      } yield ()
    }

  /** Parse a PubSub message into a `LoaderRow` (or `BadRow`) and attach `ack` action to be used after sink. */
  def parse(igluClient: Resolver[IO])(payload: Payload[IO])(implicit C: Clock[IO]): IO[Parsed] =
    LoaderRow.parse[IO](igluClient, processor)(payload.value).map {
      case Right(row) => StreamLoaderRow[IO](row, payload.ack).asRight[StreamBadRow[IO]]
      case Left(row)  => StreamBadRow[IO](row, payload.ack).asLeft[StreamLoaderRow[IO]]
    }

  /**
    * Enqueue observed types to a pre-aggregation queue and then route rows through a BigQuery sink.
    *
    * @param types A queue in which observed types will be put.
    * @return      A sink that combines enqueueing observed types with sinking rows to BigQuery and failed inserts to Pub/Sub.
    */
  def goodSink(
    r: Resources[IO],
    types: Queue[IO, Set[ShreddedType]]
  )(implicit CS: ContextShift[IO]): Pipe[IO, StreamLoaderRow[IO], Unit] =
    _.parEvalMapUnordered(MaxConcurrency) { slrow =>
      types.enqueue1(slrow.row.inventory) *>
        r.blocker.blockOn(Bigquery.insert(r, slrow.row) *> slrow.ack)
    }

  /**
    * Aggregate observed types when a specific number is reached or time passes, whichever happens first.
    * @param groupByN   Number of elements that trigger aggregation.
    * @param timeWindow Time window to aggregate over if n limit is not reached.
    * @return           A pipe that does not change the type of elements but discards non-unique values within the group.
    */
  def aggregateTypes(
    groupByN: Int,
    timeWindow: FiniteDuration
  )(implicit T: Timer[IO], C: Concurrent[IO]): Pipe[IO, Set[ShreddedType], Set[ShreddedType]] =
    _.groupWithin(groupByN, timeWindow).map(chunk => chunk.toList.toSet.flatten)
}
