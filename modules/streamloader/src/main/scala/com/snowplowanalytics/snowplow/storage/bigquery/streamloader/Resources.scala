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

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{Monitoring, Output}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.{StreamBadRow, StreamLoaderRow}
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Bigquery.FailedInsert

import cats.Applicative
import cats.effect.{Async, Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.google.cloud.bigquery.BigQuery
import com.permutive.pubsub.producer.{Model, PubsubProducer}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import fs2.{Pipe, Stream}
import io.circe.Json

import scala.concurrent.duration._

final class Resources[F[_]](
  val source: Stream[F, Payload[F]],
  val igluClient: Client[F, Json],
  val badSink: Pipe[F, StreamBadRow[F], Unit],
  val goodSink: Pipe[F, StreamLoaderRow[F], Unit],
  val metrics: Metrics[F]
)

object Resources {
  private val MaxConcurrency = Runtime.getRuntime.availableProcessors * 16
  private val GroupByN       = 10
  private val TimeWindow     = 30.seconds

  /**
    * Initialise and allocate resources, and clients containing cache and mutable state.
    * @param env parsed environment config
    * @tparam F effect type that can allocate Iglu cache
    * @return allocated `Resources`
    */
  def acquire[F[_]: Concurrent: ContextShift: InitSchemaCache: InitListCache: ConcurrentEffect: Timer](
    env: LoaderEnvironment
  ): Resource[F, Resources[F]] = {
    val clientF: F[Client[F, Json]] = Client.parseDefault[F](env.resolverJson).value.flatMap {
      case Right(client) =>
        Applicative[F].pure(client)
      case Left(error) =>
        Sync[F].raiseError[Client[F, Json]](new RuntimeException(s"Cannot decode Iglu Client: ${error.show}"))
    }

    // format: off
    for {
      blocker        <- Blocker[F]
      source         = Source.getStream[F](env.projectId, env.config.input.subscription, blocker)
      igluClient     <- Resource.liftF[F, Client[F, Json]](clientF)
      maxConcurrency <- Resource.liftF[F, Int](Sync[F].delay(Runtime.getRuntime.availableProcessors * 8))
      types          <- mkTypeSink[F](env.projectId, env.config.output.types.topic, maxConcurrency)
      bigquery       <- Resource.liftF[F, BigQuery](Bigquery.getClient)
      failedInserts  <- mkProducer[F, Bigquery.FailedInsert](env.projectId, env.config.output.failedInserts.topic, 8L, 2.seconds)
      metrics        <- Resource.liftF(mkMetricsReporter[F](blocker, env.monitoring))
      badSink        <- mkBadSink[F](env.projectId, env.config.output.bad.topic, maxConcurrency, metrics)
      goodSink       = mkGoodSink[F](blocker, env.config.output.good, bigquery, failedInserts, metrics, types)
    } yield new Resources[F](source, igluClient, badSink, goodSink, metrics)
    // format: on
  }

  /** Construct a PubSub producer. */
  private def mkProducer[F[_]: Async, A: MessageEncoder](
    projectId: String,
    topic: String,
    batchSize: Long,
    delay: FiniteDuration
  ): Resource[F, PubsubProducer[F, A]] =
    GooglePubsubProducer.of[F, A](
      Model.ProjectId(projectId),
      Model.Topic(topic),
      config = PubsubProducerConfig[F](
        batchSize         = batchSize,
        delayThreshold    = delay,
        onFailedTerminate = e => Sync[F].delay(println(s"Got error $e")).void
      )
    )

  // TODO: Can failed inserts be given their own sink like bad rows and types?

  // Acks the event after processing it as a `BadRow`
  private def mkBadSink[F[_]: Concurrent](
    projectId: String,
    topic: String,
    maxConcurrency: Int,
    metrics: Metrics[F]
  ): Resource[F, Pipe[F, StreamBadRow[F], Unit]] =
    mkProducer[F, BadRow](projectId, topic, 8L, 2.seconds).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { badRow =>
        p.produce(badRow.row) *> badRow.ack *> metrics.badCount
      }
    }

  // Does not ack the event -- it still needs to end up in one of the other targets
  private def mkTypeSink[F[_]: Concurrent](
    projectId: String,
    topic: String,
    maxConcurrency: Int
  ): Resource[F, Pipe[F, Set[ShreddedType], Unit]] =
    mkProducer[F, Set[ShreddedType]](projectId, topic, 4L, 200.millis).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { types =>
        p.produce(types).void
      }
    }

  /** Write good events through a BigQuery sink and pipe observed types through a Pub/Sub sink.
    *  Acks the event.
    */
  private def mkGoodSink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    good: Output.BigQuery,
    bigQuery: BigQuery,
    producer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    typesSink: Pipe[F, Set[ShreddedType], Unit]
  ): Pipe[F, StreamLoaderRow[F], Unit] = {
    val goodPipe: Pipe[F, StreamLoaderRow[F], Unit] = _.parEvalMapUnordered(MaxConcurrency) { slrow =>
      blocker.blockOn(
        Bigquery.insert(producer, metrics, slrow.row)(Bigquery.mkInsert(good, bigQuery)) *> slrow.ack
      )
    }

    _.observe(goodPipe).map(_.row.inventory).through(aggregateTypes[F](GroupByN, TimeWindow)).through(typesSink)
  }

  /**
    * Aggregate observed types when a specific number is reached or time passes, whichever happens first.
    * @param groupByN   Number of elements that trigger aggregation.
    * @param timeWindow Time window to aggregate over if n limit is not reached.
    * @return           A pipe that does not change the type of elements but discards non-unique values within the group.
    */
  private[streamloader] def aggregateTypes[F[_]: Timer: Concurrent](
    groupByN: Int,
    timeWindow: FiniteDuration
  ): Pipe[F, Set[ShreddedType], Set[ShreddedType]] =
    _.groupWithin(groupByN, timeWindow).map(chunk => chunk.toList.toSet.flatten)

  private def mkMetricsReporter[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    monitoringConfig: Monitoring
  ): F[Metrics[F]] =
    Metrics.build[F](blocker, monitoringConfig.statsd)
}
