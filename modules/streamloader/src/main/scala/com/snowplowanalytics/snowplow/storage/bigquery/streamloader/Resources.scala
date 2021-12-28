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

import java.nio.charset.StandardCharsets.UTF_8
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{Monitoring, Output, SinkSettings}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.{StreamBadRow, StreamLoaderRow}
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.ReportingApp
import com.snowplowanalytics.snowplow.storage.bigquery.common.Sentry
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Bigquery.FailedInsert

import cats.Applicative
import cats.effect.{Async, Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.google.cloud.bigquery.BigQuery
import com.permutive.pubsub.producer.{Model, PubsubProducer}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue
import io.circe.Json

import org.typelevel.log4cats.Logger

import scala.annotation.tailrec
import scala.concurrent.duration._

final class Resources[F[_]](
  val source: Stream[F, Payload[F]],
  val igluClient: Client[F, Json],
  val badSink: Pipe[F, StreamBadRow[F], Unit],
  val goodSink: Pipe[F, StreamLoaderRow[F], Unit],
  val metrics: Metrics[F],
  val sentry: Sentry[F]
)

object Resources {

  /**
    * Initialise and allocate resources, and clients containing cache and mutable state.
    * @param env parsed environment config
    * @tparam F effect type that can allocate Iglu cache
    * @return allocated `Resources`
    */
  def acquire[F[_]: Concurrent: ContextShift: InitSchemaCache: InitListCache: ConcurrentEffect: Timer: Logger](
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
      source         = Source.getStream[F](env.projectId, env.config.input.subscription, blocker, env.config.consumerSettings)
      igluClient     <- Resource.eval[F, Client[F, Json]](clientF)
      metrics        <- Resource.eval(mkMetricsReporter[F](blocker, env.monitoring))
      types          <- mkTypeSink[F](env.projectId, env.config.output.types.topic, env.config.sinkSettings.types, metrics)
      bigquery       <- Resource.eval[F, BigQuery](Bigquery.getClient)
      failedInserts  <- mkProducer[F, Bigquery.FailedInsert](
        env.projectId,
        env.config.output.failedInserts.topic,
        env.config.sinkSettings.failedInserts.producerBatchSize,
        env.config.sinkSettings.failedInserts.producerDelayThreshold
      )
      queue          <- Resource.eval(Queue.bounded[F, StreamLoaderRow[F]](env.config.sinkSettings.good.bqWriteRequestOverflowQueueMaxSize))
      badSink        <- mkBadSink[F](env.projectId, env.config.output.bad.topic, env.config.sinkSettings.bad.sinkConcurrency, metrics, env.config.sinkSettings.bad)
      sentry         <- Sentry.init(env.monitoring.sentry)
      goodSink       = mkGoodSink[F](blocker, env.config.output.good, bigquery, failedInserts, metrics, types, queue, env.config.sinkSettings.good, env.config.sinkSettings.types)
    } yield new Resources[F](source, igluClient, badSink, goodSink, metrics, sentry)
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
    metrics: Metrics[F],
    sinkSettingsBad: SinkSettings.Bad
  ): Resource[F, Pipe[F, StreamBadRow[F], Unit]] =
    mkProducer[F, BadRow](
      projectId,
      topic,
      sinkSettingsBad.producerBatchSize,
      sinkSettingsBad.producerDelayThreshold
    ).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { badRow =>
        p.produce(badRow.row) *> badRow.ack *> metrics.badCount
      }
    }

  // Does not ack the event -- it still needs to end up in one of the other targets
  private def mkTypeSink[F[_]: Concurrent](
    projectId: String,
    topic: String,
    sinkSettingsTypes: SinkSettings.Types,
    metrics: Metrics[F]
  ): Resource[F, Pipe[F, Set[ShreddedType], Unit]] =
    mkProducer[F, Set[ShreddedType]](
      projectId,
      topic,
      sinkSettingsTypes.producerBatchSize,
      sinkSettingsTypes.producerDelayThreshold
    ).map { p =>
      _.parEvalMapUnordered(sinkSettingsTypes.sinkConcurrency) { types =>
        p.produce(types).void *> metrics.typesCount(types.size)
      }
    }

  /** Write good events through a BigQuery sink and pipe observed types through a Pub/Sub sink.
    * Acks the events.
    *
    * The input Stream of events is transformed into a Stream of batches of events (List[StreamLoaderRow]).
    * Each batch respects the Streaming APIs recommended number of records per request.
    * Each BQ write request also has a size limit of 10MB. In case a batch's total size exceeds this limit, we use
    * as many events as possible (while staying below it) in the write request, and re-insert the remaining events
    * back into the original Stream of events.
    *
    * @param bufferOverflowQueue A Queue in which put events that are left over from a batch after taking as many as
    *                            possible without breaching the streaming inserts request size limit
    */
  private def mkGoodSink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    good: Output.BigQuery,
    bigQuery: BigQuery,
    producer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    typeSink: Pipe[F, Set[ShreddedType], Unit],
    bufferOverflowQueue: Queue[F, StreamLoaderRow[F]],
    sinkSettingsGood: SinkSettings.Good,
    sinkSettingsTypes: SinkSettings.Types
  ): Pipe[F, StreamLoaderRow[F], Unit] = {
    val goodPipe: Pipe[F, StreamLoaderRow[F], Unit] = {
      _.merge(bufferOverflowQueue.dequeue)
        .through(batch(sinkSettingsGood.bqWriteRequestThreshold, sinkSettingsGood.bqWriteRequestTimeout))
        .parEvalMapUnordered(sinkSettingsGood.sinkConcurrency) { slrows =>
          val (remaining, toInsert) = splitBy(slrows, getSize[F], sinkSettingsGood.bqWriteRequestSizeLimit)
          remaining.traverse_(bufferOverflowQueue.enqueue1) *>
            Bigquery.insert(producer, metrics, toInsert.map(_.row))(Bigquery.mkInsert(good, bigQuery, blocker)) *> toInsert
            .traverse_(_.ack)
        }
    }

    // There is no guarantee that any side effects from `observe(goodPipe)` will be executed before side effects in `through(typeSink)`.
    _.observe(goodPipe)
      .map(_.row.inventory)
      .through(aggregateTypes[F](sinkSettingsTypes.batchThreshold, sinkSettingsTypes.batchTimeout))
      .through(typeSink)
  }

  /**
    * Splits a List of As into two Lists, using the provided getSize function.
    * Used to split a List[StreamLoaderRow] into a (remaining, toInsert) Lists.
    * The max size of a Pub/Sub message is 10MB, so a lower threshold might result in events that get forever trapped in the stream.
    * @param batch A List of As (used with StreamLoaderRow)
    * @param getSize A function to get the size of a single A
    * @param threshold A limit to how big the toInsert List can be, as measured by toInsert.map(getSize).sum
    * @tparam A The type of element in the original collection (used with StreamLoaderRow)
    * @return A tuple of Lists. The second List contains all As that could be inserted into it without breaching the threshold. The first List contains the remaining As.
    */
  private[streamloader] def splitBy[A](batch: List[A], getSize: A => Int, threshold: Int): (List[A], List[A]) = {
    val originalBatch = batch
    @tailrec
    def go(batch: List[A], acc: (List[A], Int, Boolean)): (List[A], List[A]) = {
      val (buffer, totalSize, isFull) = acc
      batch match {
        case _ if isFull => (originalBatch.drop(buffer.length), buffer.reverse)
        case Nil         => (List.empty[A], buffer.reverse)
        case h :: t =>
          val currSize = getSize(h)
          if (currSize + totalSize > threshold) go(t, (buffer, totalSize, true))
          else go(t, (h :: buffer, currSize + totalSize, false))
      }
    }

    go(batch, (List.empty[A], 0, false))
  }

  private def getSize[F[_]]: StreamLoaderRow[F] => Int = _.row.data.toString.getBytes(UTF_8).length

  /**
    * Turn a Stream[A] into a Stream[List[A] ] by batching As into a List[A] with size n unless time t passes with no more incoming elements.
    * @param n How many As to put in the batch.
    * @param t How long to wait before finalising the batch
    */
  private[streamloader] def batch[F[_]: Timer: Concurrent, A](
    n: Int,
    t: FiniteDuration
  ): Pipe[F, A, List[A]] =
    _.groupWithin(n, t).map(_.toList)

  /**
    * Aggregate observed types when a specific number is reached or time passes, whichever happens first.
    * @param n Number of elements that trigger aggregation.
    * @param t Time window to aggregate over if n limit is not reached.
    * @return  A pipe that does not change the type of elements but discards non-unique values within the group.
    */
  private[streamloader] def aggregateTypes[F[_]: Timer: Concurrent](
    n: Int,
    t: FiniteDuration
  ): Pipe[F, Set[ShreddedType], Set[ShreddedType]] =
    _.through(batch(n, t)).map(_.toSet.flatten)

  private def mkMetricsReporter[F[_]: ConcurrentEffect: ContextShift: Timer: Logger](
    blocker: Blocker,
    monitoringConfig: Monitoring
  ): F[Metrics[F]] =
    Metrics.build[F](blocker, monitoringConfig, ReportingApp.StreamLoader)
}
