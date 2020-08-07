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

import com.google.cloud.bigquery.BigQuery

import scala.concurrent.duration._

import cats.effect.{Concurrent, ContextShift, ExitCode, IO, Timer}
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Source.PubsubSource
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.{Bigquery, PubSub}
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

  private val MaxConcurrency = Runtime.getRuntime.availableProcessors * 16
  private val GroupByN       = 10
  private val TimeWindow     = 5.minutes

  implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  def run(e: Environment)(implicit cs: ContextShift[IO], c: Concurrent[IO], T: Timer[IO]): IO[ExitCode] = {
    val source = new PubsubSource(e)(cs, c)

    val eventStream: Stream[IO, Either[BadRow, StreamLoaderRow]] =
      source.getStream.evalMap(StreamLoaderRow.parse(e.resolverJson))

    for {
      client <- Bigquery.getClient
      queue  <- Queue.bounded[IO, Set[ShreddedType]](MaxConcurrency)
      sinkBadGood = eventStream.observeEither[BadRow, StreamLoaderRow](
        badSink(e),
        goodSink(queue, _.inventory, bigquerySink(e, client))
      )
      sinkTypes = queue.dequeue.through(aggregateTypes(GroupByN, TimeWindow)).through(typesSink(e))
      _ <- Stream(sinkTypes, sinkBadGood).parJoin(MaxConcurrency).compile.drain
    } yield ExitCode.Success
  }

  // TODO: Maybe sinks can be generalised?

  /**
    * Sink bad rows to Pub/Sub.
    * @param e Parsed common environment.
    * @return A sink that writes bad rows to Pub/Sub.
    */
  def badSink(e: Environment): Pipe[IO, BadRow, Unit] =
    _.parEvalMapUnordered(MaxConcurrency)(badRow =>
      PubSub.write(PubSubOutput.WriteBadRow(badRow), e.config.projectId, e.config.badRows)
    )

  /**
    * Sink types to Pub/Sub.
    * @param e Parsed common environment.
    * @return A sink that writes observed types to Pub/Sub (to be consumed by Mutator).
    */
  def typesSink(e: Environment): Pipe[IO, Set[ShreddedType], Unit] =
    _.parEvalMapUnordered(MaxConcurrency)(typesSet =>
      PubSub.write(PubSubOutput.WriteObservedTypes(typesSet), e.config.projectId, e.config.typesTopic)
    )

  /**
    * Sink rows to BigQuery and failed inserts to Pub/Sub.
    * @param e Parsed common environment.
    * @param c BigQuery client.
    * @return A sink that writes rows to a BigQuery table and routes failed inserts to Pub/Sub (to be consumed by Repeater).
    */
  def bigquerySink(e: Environment, c: BigQuery): Pipe[IO, StreamLoaderRow, Unit] =
    _.parEvalMapUnordered(MaxConcurrency)(loaderRow => Bigquery.insert(loaderRow, e, c))

  /**
    * Enqueue observed types to a pre-aggregation queue and then route rows through a BigQuery sink.
    *
    * @param types        A queue in which observed types will be put.
    * @param getTypes     A function to extract types from a [[StreamLoaderRow]].
    * @param bigquerySink A sink that writes rows to a BigQuery table and routes failed inserts to Pub/Sub.
    * @return             A sink that combines enqueing observed types with sinking rows to BigQuery and failed inserts to Pub/Sub.
    */
  def goodSink(
    types: Queue[IO, Set[ShreddedType]],
    getTypes: StreamLoaderRow => Set[ShreddedType],
    bigquerySink: Pipe[IO, StreamLoaderRow, Unit]
  ): Pipe[IO, StreamLoaderRow, Unit] =
    _.parEvalMapUnordered(MaxConcurrency) { lr =>
      types.enqueue1(getTypes(lr)) *> IO(lr)
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
