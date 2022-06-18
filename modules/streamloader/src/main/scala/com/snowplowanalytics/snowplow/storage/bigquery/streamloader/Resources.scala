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

import java.nio.charset.StandardCharsets.UTF_8
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{Monitoring, Output, SinkSettings}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.{StreamBadRow, StreamLoaderRow}
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.ReportingApp
import com.snowplowanalytics.snowplow.storage.bigquery.common.Sentry
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Bigquery.FailedInsert

import cats.Applicative
import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import com.google.cloud.bigquery.BigQuery
import com.permutive.pubsub.producer.{Model, PubsubProducer}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import fs2.{Pipe, Stream}
import io.circe.Json

import org.typelevel.log4cats.Logger

import scala.annotation.tailrec
import scala.concurrent.duration._

final class Resources[F[_]](
  val source: Stream[F, Payload[F]],
  val igluClient: Client[F, Json],
  val badSink: Pipe[F, StreamBadRow[F], Nothing],
  val goodSink: Pipe[F, StreamLoaderRow[F], Nothing],
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
  def acquire[F[_]: Async: InitSchemaCache: InitListCache: Logger](
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
      igluClient     <- Resource.eval[F, Client[F, Json]](clientF)
      metrics        <- Resource.eval(mkMetricsReporter[F](env.monitoring))
      types          <- mkTypeSink[F](env.projectId, env.config.output.types.topic, env.config.sinkSettings.types, metrics)
      bigquery       <- Resource.eval[F, BigQuery](Bigquery.getClient(env.config.retrySettings, env.projectId))
      failedInserts  <- mkProducer[F, Bigquery.FailedInsert](
        env.projectId,
        env.config.output.failedInserts.topic,
        env.config.sinkSettings.failedInserts.producerBatchSize,
        env.config.sinkSettings.failedInserts.producerDelayThreshold
      )
      badSink        <- mkBadSink[F](env.projectId, env.config.output.bad.topic, env.config.sinkSettings.bad.sinkConcurrency, metrics, env.config.sinkSettings.bad)
      sentry         <- Sentry.init(env.monitoring.sentry)
      goodSink       = mkGoodSink[F](env.config.output.good, bigquery, failedInserts, metrics, types, env.config.sinkSettings.good, env.config.sinkSettings.types)
      source         = Source.getStream[F](env.projectId, env.config.input.subscription, env.config.consumerSettings)
    } yield new Resources[F](source, igluClient, badSink, goodSink, metrics, sentry)
    // format: on
  }

  /** Construct a PubSub producer. */
  private def mkProducer[F[_]: Async: Logger, A: MessageEncoder](
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
        onFailedTerminate = e => Logger[F].error(e)(s"Error in PubSub producer")
      )
    )

  // TODO: Can failed inserts be given their own sink like bad rows and types?

  // Acks the event after processing it as a `BadRow`
  private def mkBadSink[F[_]: Async: Logger](
    projectId: String,
    topic: String,
    maxConcurrency: Int,
    metrics: Metrics[F],
    sinkSettingsBad: SinkSettings.Bad
  ): Resource[F, Pipe[F, StreamBadRow[F], Nothing]] =
    mkProducer[F, BadRow](
      projectId,
      topic,
      sinkSettingsBad.producerBatchSize,
      sinkSettingsBad.producerDelayThreshold
    ).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { badRow =>
        p.produce(badRow.row) *> badRow.ack *> metrics.badCount
      }.drain
    }

  // Does not ack the event -- it still needs to end up in one of the other targets
  private def mkTypeSink[F[_]: Async: Logger](
    projectId: String,
    topic: String,
    sinkSettingsTypes: SinkSettings.Types,
    metrics: Metrics[F]
  ): Resource[F, Pipe[F, Set[ShreddedType], Nothing]] =
    mkProducer[F, Set[ShreddedType]](
      projectId,
      topic,
      sinkSettingsTypes.producerBatchSize,
      sinkSettingsTypes.producerDelayThreshold
    ).map { p =>
      _.parEvalMapUnordered(sinkSettingsTypes.sinkConcurrency) { types =>
        p.produce(types).void *> metrics.typesCount(types.size)
      }.drain
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
  private def mkGoodSink[F[_]: Async](
    good: Output.BigQuery,
    bigQuery: BigQuery,
    producer: PubsubProducer[F, FailedInsert],
    metrics: Metrics[F],
    typeSink: Pipe[F, Set[ShreddedType], Nothing],
    sinkSettingsGood: SinkSettings.Good,
    sinkSettingsTypes: SinkSettings.Types
  ): Pipe[F, StreamLoaderRow[F], Nothing] = {
    val goodPipe: Pipe[F, StreamLoaderRow[F], Nothing] = {
      _.through(batchWithin(sinkSettingsGood.bqWriteRequestThreshold, sinkSettingsGood.bqWriteRequestTimeout))
        .through(batchBySize(getSize[F], sinkSettingsGood.bqWriteRequestSizeLimit))
        .parEvalMapUnordered(sinkSettingsGood.sinkConcurrency) { slrows =>
          Bigquery.insert(producer, metrics, slrows.map(_.row))(Bigquery.mkInsert(good, bigQuery)) *>
            slrows.traverse_(_.ack)
        }
        .drain
    }

    // There is no guarantee that any side effects from `observe(goodPipe)` will be executed before side effects in `through(typeSink)`.
    _.observe(goodPipe)
      .map(_.row.inventory)
      .through(aggregateTypes[F](sinkSettingsTypes.batchThreshold, sinkSettingsTypes.batchTimeout))
      .through(typeSink)
  }

  /**
    * Batches a List of As into sub-Lists, using the provided getSize function.
    * Used to split an unbounded List[StreamLoaderRow] into smaller Lists
    * The max size of a Pub/Sub message is 10MB, so a lower threshold might result in events that get forever trapped in the stream.
    * @param getSize A function to get the size of a single A
    * @param threshold A limit to how big each sub-List can be, as measured by summing over calls to `getSize`
    * @tparam A The type of element in the original collection (used with StreamLoaderRow)
    * @return A Pipe from List to List. Each output list may be inserted without breaching the threshold.
    */
  private[streamloader] def batchBySize[F[_], A](getSize: A => Int, threshold: Int): Pipe[F, List[A], List[A]] = {
    @tailrec
    def go(accHead: List[A], accTail: List[List[A]], accSize: Int, input: List[A]): List[List[A]] =
      input match {
        case Nil => accHead :: accTail
        case head :: tail =>
          val headSize = getSize(head)
          if (headSize + accSize > threshold)
            go(List(head), accHead :: accTail, headSize, tail)
          else
            go(head :: accHead, accTail, headSize + accSize, tail)
      }

    _.flatMap(as => Stream.emits(go(Nil, Nil, 0, as).reverse)).map(_.reverse).filter(_.nonEmpty)
  }

  private def getSize[F[_]]: StreamLoaderRow[F] => Int = _.row.data.toString.getBytes(UTF_8).length

  /**
    * Turn a Stream[A] into a Stream[List[A] ] by batching As into a List[A] with size n unless time t passes with no more incoming elements.
    * @param n How many As to put in the batch.
    * @param t How long to wait before finalising the batch
    */
  private[streamloader] def batchWithin[F[_]: Async, A](
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
  private[streamloader] def aggregateTypes[F[_]: Async](
    n: Int,
    t: FiniteDuration
  ): Pipe[F, Set[ShreddedType], Set[ShreddedType]] =
    _.through(batchWithin(n, t)).map(_.toSet.flatten)

  private def mkMetricsReporter[F[_]: Async: Logger](
    monitoringConfig: Monitoring
  ): F[Metrics[F]] =
    Metrics.build[F](monitoringConfig, ReportingApp.StreamLoader)
}
