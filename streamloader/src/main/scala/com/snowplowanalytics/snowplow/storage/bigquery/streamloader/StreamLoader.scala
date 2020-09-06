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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import scala.concurrent.duration._

import cats.implicits._
import cats.effect.{Clock, Concurrent, ContextShift, IO, Timer}

import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment

import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Source.{Payload, PubsubSource}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.Bigquery
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.PubSub.PubSubOutput

object StreamLoader {

  //      TOPOLOGY OVERVIEW
  //                                            +-----------+
  //                                 bad sink   |   PubSub  |
  //                              +-------------|   (bad)   |
  //                              |             +-----------+
  //                              |
  //  +------------+          +--------+                                       aggregate
  //  |            |  parse   | (bad,  |                  enqueue   +-------+  >> sink     +--------+
  //  | enriched   | -------- | good)  |                  types     |types  |  types       | PubSub |
  //  |            |          |        |               +------------|queue  | -------------| (types)|
  //  +------------+          +--------+               |            +-------+              +--------+
  //                              |                    |
  //                              |                    |
  //                              |             +------------+                              +---------+
  //                              |             |            |              failed inserts  | PubSub  |
  //                              +-------------| good sink  |             +----------------| (failed |
  //                                            |            |             |                | inserts)|
  //                                            +------|-----+             |                +---------+
  //                                                   |                   |
  //                                                   |             +-----|----+
  //                                                   |             | BigQuery |
  //                                                   +-------------| sink     |
  //                                                                 +-----|----+
  //                                                                       |
  //                                                                       |                 +--------+
  //                                                                       |     insert      |BigQuery|
  //                                                                       +-----------------|(events)|
  //                                                                                         +--------+

  val processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  /** PubSub message with successfully parsed row, ready to be inserted into BQ */
  case class StreamLoaderRow[F[_]](row: LoaderRow, ack: F[Unit])

  /** PubSub message with a row that failed parsing */
  case class StreamBadRow[F[_]](row: BadRow, ack: F[Unit])

  type Parsed = Either[StreamBadRow[IO], StreamLoaderRow[IO]]

  private val MaxConcurrency = Runtime.getRuntime.availableProcessors * 16
  private val GroupByN       = 10
  private val TimeWindow     = 5.minutes

  def run(e: Environment)(implicit cs: ContextShift[IO], c: Concurrent[IO], T: Timer[IO]): IO[Unit] =
    Resources.acquire(e).use { resources =>
      val eventStream: Stream[IO, Parsed] =
        new PubsubSource(e).getStream.evalMap(parse(resources.igluClient.resolver))

      for {
        queue  <- Queue.bounded[IO, Set[ShreddedType]](MaxConcurrency)
        sinkBadGood = eventStream.observeEither[StreamBadRow[IO], StreamLoaderRow[IO]](
          badSink(resources),
          goodSink(queue, bigquerySink(resources))
        )
        sinkTypes = queue.dequeue.through(aggregateTypes(GroupByN, TimeWindow)).through(typesSink(resources))
        _ <- Stream(sinkTypes, sinkBadGood).parJoin(MaxConcurrency).compile.drain
      } yield ()
    }

  /** Parse common `LoaderRow` (or `BadRow`) and attach `ack` action to be used after sink */
  def parse(igluClient: Resolver[IO])(payload: Payload)(implicit C: Clock[IO]): IO[Parsed] =
    LoaderRow.parse[IO](igluClient, processor)(payload.value).map {
      case Right(row) => StreamLoaderRow[IO](row, payload.ack).asRight[StreamBadRow[IO]]
      case Left(row) => StreamBadRow[IO](row, payload.ack).asLeft[StreamLoaderRow[IO]]
    }

  /**
    * Sink bad rows to Pub/Sub.
    * @param r allocated set of Loader resources
    * @return A sink that writes bad rows to Pub/Sub.
    */
  def badSink(r: Resources[IO])(implicit cs: ContextShift[IO]): Pipe[IO, StreamBadRow[IO], Unit] =
    _.parEvalMapUnordered(MaxConcurrency) { badRow =>
      r.pubsub.produce(PubSubOutput.WriteBadRow(badRow.row)) *> badRow.ack
    }

  /**
    * Sink types to Pub/Sub.
    * @param r allocated set of Loader resources
    * @return A sink that writes observed types to Pub/Sub (to be consumed by Mutator).
    */
  def typesSink(r: Resources[IO])(implicit cs: ContextShift[IO]): Pipe[IO, Set[ShreddedType], Unit] =
    _.parEvalMapUnordered(MaxConcurrency)(typesSet =>
      r.pubsub.produce(PubSubOutput.WriteObservedTypes(typesSet)).void
    )

  /**
    * Sink rows to BigQuery and failed inserts to Pub/Sub.
    * @param r allocated set of Loader resources
    * @return A sink that writes rows to a BigQuery table and routes failed inserts to Pub/Sub (to be consumed by Repeater).
    */
  def bigquerySink(r: Resources[IO])(implicit cs: ContextShift[IO]): Pipe[IO, StreamLoaderRow[IO], Unit] =
    _.parEvalMapUnordered(MaxConcurrency) { row => Bigquery.insert(r, row.row) *> row.ack }

  /**
    * Enqueue observed types to a pre-aggregation queue and then route rows through a BigQuery sink.
    *
    * @param types        A queue in which observed types will be put.
    * @param bigquerySink A sink that writes rows to a BigQuery table and routes failed inserts to Pub/Sub.
    * @return             A sink that combines enqueing observed types with sinking rows to BigQuery and failed inserts to Pub/Sub.
    */
  def goodSink(
    types: Queue[IO, Set[ShreddedType]],
    bigquerySink: Pipe[IO, StreamLoaderRow[IO], Unit]
  )(implicit cs: ContextShift[IO]): Pipe[IO, StreamLoaderRow[IO], Unit] =
    _.parEvalMapUnordered(MaxConcurrency) { row =>
      types.enqueue1(row.row.inventory).as(row)
    }.through(bigquerySink)

  /**
    * Aggregate observed types when a specific number is reached or time passes, whichever happens first.
    * @param groupByN Number of elements that trigger aggregation.
    * @param timeWindow Time window to aggregate over if n limit is not reached.
    * @return A pipe that does not change the type of elements but discards non-unique values within the group.
    */
  def aggregateTypes(
    groupByN: Int,
    timeWindow: FiniteDuration
  )(implicit T: Timer[IO], C: Concurrent[IO]): Pipe[IO, Set[ShreddedType], Set[ShreddedType]] =
    _.groupWithin(groupByN, timeWindow).map(chunk => chunk.toList.toSet.flatten)
}
