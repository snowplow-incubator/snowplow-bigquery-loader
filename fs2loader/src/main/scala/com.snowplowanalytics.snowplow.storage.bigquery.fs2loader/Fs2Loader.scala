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
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.{Bigquery, PubSub}
import com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks.PubSub.{WriteBadRow, WriteObservedTypes}
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow
import org.slf4j.LoggerFactory
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.google.cloud.bigquery.BigQuery

import scala.concurrent.duration._

object Fs2Loader {

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
//                              +-------------| good pipe  |             +----------------| (failed |
//                                            |            |             |                | inserts)|
//                                            +------|-----+             |                +---------+
//                                                   |                   |
//                                                   |             +-----|----+
//                                                   |             | BigQuery |
//                                                   +-------------| pipe     |
//                                                                 +-----|----+
//                                                                       |
//                                                                       |                 +--------+
//                                                                       |                 |BigQuery|
//                                                                       +-----------------|(events)|
//                                                                                         +--------+

  private val MaxConcurrency = 10
  private val GroupByN       = 10
  private val TimeWindow     = 10.seconds

  def badSink(badRow: BadRow)(env: Environment): IO[Unit] =
    PubSub.sink(env.config.projectId, env.config.badRows)(WriteBadRow(badRow))

  def goodPipe[LR, ST, Unit](
    observedTypes: Queue[IO, Set[ST]],
    getTypes: LR => Set[ST],
    bigqueryPipe: LR => IO[Unit]
  ): Pipe[IO, LR, Unit] =
    _.evalMap { lr =>
      observedTypes.enqueue1(getTypes(lr)) *> bigqueryPipe(lr)
    }

  /**
    * Aggregate observed types when a specific number is reached or time passes, whichever happens first.
    * @param groupByN Number of elements that trigger aggregation.
    * @param timeWindow Time window to aggregate over if n limit is not reached.
    * @tparam ST In a prod setting, this should be [[ShreddedType]].
    * @return A pipe that does not change the type of elements but discards non-unique values within the group.
    */
  def aggregateTypes[ST](
    groupByN: Int,
    timeWindow: FiniteDuration
  )(implicit T: Timer[IO], C: Concurrent[IO]): Pipe[IO, Set[ST], Set[ST]] =
    _.groupWithin(groupByN, timeWindow).map { chunk =>
      chunk.toList.toSet.flatten
    }

  def typesSink(types: Stream[IO, Set[ShreddedType]])(env: Environment): Stream[IO, Unit] =
    types.evalMap(set => PubSub.sink(env.config.projectId, env.config.typesTopic)(WriteObservedTypes(set)))

  def bigqueryPipe(loaderRow: LoaderRow)(env: Environment)(client: BigQuery): IO[Unit] =
    Bigquery.insert[IO](client, loaderRow)(env)

  def run(
    env: Environment
  )(static: Boolean = true)(implicit cs: ContextShift[IO], c: Concurrent[IO], T: Timer[IO]): IO[ExitCode] = {
    val source = {
      if (static) {
        new StaticSource(6)(c)
      } else {
        new PubsubSource(1)(env)(cs, c)
      }
    }

    val eventStream: Stream[IO, Either[BadRow, LoaderRow]] =
      source.getStream.map(LoaderRow.parse(env.resolverJson))

    for {
      client <- Bigquery.getClient
      queue  <- Queue.bounded[IO, Set[ShreddedType]](MaxConcurrency)
      sinkBadGood = eventStream.observeEither[BadRow, LoaderRow](
        _.evalMap(br => badSink(br)(env)),
        goodPipe[LoaderRow, ShreddedType, Unit](queue, _.inventory, bigqueryPipe(_)(env)(client))
      )
      sinkTypes = queue.dequeue.through(aggregateTypes(GroupByN, TimeWindow)).through(typesSink(_)(env))
      _ <- Stream(sinkTypes, sinkBadGood).parJoin(Int.MaxValue).compile.drain
    } yield ExitCode.Success
  }

//  /**
//    * Extract observed types from loader rows and add aggregates to a queue.
//    *
//    * @param queue     A queue to add aggregated types to.
//    * @param getTypes A function that extracts types from loader rows.
//    * @param groupByN Number of elements that trigger aggregation.
//    * @param timeWindow Time window to aggregate over if n limit is not reached.
//    * @tparam LR The type of the row, should be [[LoaderRow]] in prod.
//    * @tparam ST The type of the observed types, should be [[ShreddedType]] in prod.
//    * @tparam U The return type of the load function, should be [[Unit]] in prod.
//    * @return A pipe that sinks observed types to a queue.
//    */
//  def enqueueTypes[LR, ST, U](
//    queue: Queue[IO, Set[ST]],
//    getTypes: LR => Set[ST],
//    groupByN: Int,
//    timeWindow: FiniteDuration,
//    enqueueF: Pipe[IO, Set[ST], U]
//  )(implicit T: Timer[IO], C: Concurrent[IO]): Pipe[IO, LR, U] =
//    _.map(getTypes).through[IO, Set[ST]](aggregateTypes[ST](groupByN, timeWindow)(T, C)).through(enqueueF)
//
//  private def enqueueF[ST](q: Queue[IO, Set[ST]]) = q.enqueue

//  /**
//    *
//    * @param env Parsed common environment.
//    * @param typesQueue A queue of aggregated types.
//    * @param dequeueF A pipe that sinks observed types.
//    * @tparam ST The type of the observed types, should be [[ShreddedType]] in prod.
//    * @tparam U The return type of the load function, should be [[Unit]] in prod.
//    * @return A stream of IO to sink observed types.
//    */
//  def dequeueTypes[ST, U](env: Option[Environment] = None)(
//    typesQueue: Queue[IO, Set[ST]],
//    dequeueF: Pipe[IO, Set[ST], U]
//  )(implicit C: Concurrent[IO]): Stream[IO, U] =
//    typesQueue.dequeue.through(dequeueF)

//  private def logTypes(types: Stream[IO, Set[ShreddedType]]) =
//    types.evalMap { set =>
//      IO.delay(println("Hello from types sink.")) *> IO.delay(log(toPayload(set).noSpaces))
//    }
//  private def typesToPubsub(env: Environment)(types: Stream[IO, Set[ShreddedType]]) =
//    types.evalMap(set => PubSub.sink(env.config.projectId, env.config.typesTopic)(WriteObservedTypes(set)))

//  /**
//    * Sink rows to BigQuery.
//    * @param sink A BQ load function.
//    * @tparam LR The type of the row, should be [[LoaderRow]] in prod.
//    * @tparam U The return type of the load function, should be [[Unit]] in prod.
//    * @return A pipe that sinks rows to BigQuery.
//    */
//  def bigquerySink[LR, ST, U](typesQueue: Queue[IO, Set[ST]], f: LR => Set[ST], sink: LR => IO[U])(
//    implicit C: Concurrent[IO]
//  ): Pipe[IO, LR, U] =
//    _.parEvalMapUnordered(MaxConcurrency) { lr =>
//      IO.delay(println("Hello from good sink.")) *>
//        typesQueue.enqueue1(f(lr)) *>
//        sink(lr)
//    }

//  private def logGood(loaderRow: LoaderRow): IO[Unit] = IO.delay(log(loaderRow.toString))

//  private def goodRowsToBigquery(loaderRow: LoaderRow)(env: Environment)(client: BigQuery): IO[Unit] =
//    Bigquery.insert[IO](client, env.config.datasetId, env.config.tableId, loaderRow)
//
//  /**
//    *
//    * @param env Parsed common environment.
//    * @param sink A pipe that sinks bad rows.
//    * @tparam BR The type of bad rows, should be [[BadRow]] in prod.
//    * @tparam U The return type of the sink function, should be [[Unit]] in prod.
//    * @return A stream of IO to sink bad rows.
//    */
//  def badSink[BR, U](env: Option[Environment] = None)(sink: BR => IO[U])(implicit C: Concurrent[IO]): Pipe[IO, BR, U] =
//    _.evalMap { br =>
//      IO.delay(println("Hello from bad sink.")) *> sink(br)
//    }
//
//  private def logBad(badRow: BadRow): IO[Unit] =
//    IO.delay(log(badRow.toString))
//  private def badRowsToPubsub(badRow: BadRow)(env: Environment): IO[Unit] =
//    PubSub.sink(env.config.projectId, env.config.badRows)(WriteBadRow(badRow))
//
//  private def log(s: String): Unit = LoggerFactory.getLogger("sink").info(s)

// We do not need this and I couldn't find a way to make it work. But leaving it here for now in case we want to revisit.
//  /**
//    * Sink aggregate types to a dedicated queue and good rows to BigQuery.
//    * @param typesQueue A queue to add aggregated types to.
//    * @param getTypes A function that extracts types from loader rows.
//    * @param groupByN Number of elements that trigger aggregation.
//    * @param timeWindow Time window to aggregate over if n limit is not reached.
//    * @param sink A BQ load function.
//    * @tparam LR The type of the row, should be [[LoaderRow]] in prod.
//    * @tparam U The return type of the load function, should be [[Unit]] in prod.
//    * @tparam ST The type of the observed types, should be [[ShreddedType]] in prod.
//    * @return A pipe that sinks observed types to a queue and rows to BigQuery.
//    */
//  def goodSink[LR, U, ST](
//    typesQueue: Queue[IO, Set[ST]],
//    getTypes: LR => Set[ST],
//    groupByN: Int,
//    timeWindow: FiniteDuration,
//    enqueueF: Pipe[IO, Set[ST], U],
//    sink: LR => IO[U]
//  )(implicit C: Concurrent[IO], T: Timer[IO]): Pipe[IO, LR, U] =
//    loaderRows =>
//      loaderRows
//        .through(enqueueTypes[LR, ST, U](typesQueue, getTypes, groupByN, timeWindow, enqueueF))
//        .concurrently(
//          loaderRows.through(
//            bigquerySink(sink)
//          )
//        )

}
