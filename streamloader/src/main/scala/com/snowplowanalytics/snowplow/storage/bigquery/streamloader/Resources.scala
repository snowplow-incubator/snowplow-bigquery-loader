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

import cats.Applicative
import cats.implicits._
import cats.effect.{Async, Blocker, Concurrent, Resource, Sync}

import io.circe.Json

import fs2.{Pipe, Stream}

import com.google.cloud.bigquery.BigQuery

import com.permutive.pubsub.producer.Model
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.{Bigquery, PubSub}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.PubSub.{Producer, PubSubOutput}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.StreamBadRow

class Resources[F[_]](val failedInsertsProducer: PubSub.Producer[F],
                      val badRowsSink: Pipe[F, StreamBadRow[F], Unit],
                      val typesSink: Pipe[F, Set[ShreddedType], Unit],
                      val maxConcurrrency: Int,
                      val bigQuery: BigQuery,
                      val igluClient: Client[F, Json],
                      val env: Environment,
                      val blocker: Blocker)

object Resources {
  /**
    * Initialise and allocate resources, and clients containing cache and mutable state
    * @param env parsed environment config
    * @tparam F effect type that can allocate Iglu cache
    * @return allocated `Resources`
    */
  def acquire[F[_]: Concurrent: InitSchemaCache: InitListCache](env: Environment): Resource[F, Resources[F]] = {
    val clientF: F[Client[F, Json]] = Client.parseDefault[F](env.resolverJson).value.flatMap {
      case Right(client) =>
        Applicative[F].pure(client)
      case Left(error) =>
        Sync[F].raiseError[Client[F, Json]](new RuntimeException(s"Cannot decode Iglu Client: ${error.show}"))
    }

    for {
      maxConcurrency <- Resource.liftF[F, Int](Sync[F].delay(Runtime.getRuntime.availableProcessors * 8))
      failedInserts <- mkProducer[F](env.config.projectId, env.config.failedInserts, 8, 2.seconds)
      badRows <- mkBadRowsSink[F](env.config.projectId, env.config.badRows, maxConcurrency)
      types <- mkTypesSink[F](env.config.projectId, env.config.failedInserts, maxConcurrency)
      bigquery <- Resource.liftF(Bigquery.getClient)
      client <- Resource.liftF(clientF)
      blocker <- Blocker[F]
    } yield new Resources[F](failedInserts, badRows, types, maxConcurrency, bigquery, client, env, blocker)
  }

  /** Constructor for PubSub producer (reader) */
  def mkProducer[F[_]: Async](projectId: String,
                              topic: String,
                              batchSize: Long,
                              delay: FiniteDuration): Resource[F, Producer[F]] =
    GooglePubsubProducer
      .of[F, PubSubOutput](
        Model.ProjectId(projectId),
        Model.Topic(topic),
        config = PubsubProducerConfig[F](
          // TODO: Get rid of magic numbers
          batchSize         = batchSize,
          delayThreshold    = delay,
          onFailedTerminate = e => Sync[F].delay(println(s"Got error $e")).void
        )
      )

  def mkBadRowsSink[F[_]: Concurrent](project: String, topic: String, maxConcurrency: Int): Resource[F, Pipe[F, StreamBadRow[F], Unit]] =
    mkProducer[F](project, topic, 8L, 2.seconds).map { p =>
      (s: Stream[F, StreamBadRow[F]]) =>
        s.parEvalMapUnordered(maxConcurrency) { badRow =>
          p.produce(PubSubOutput.WriteBadRow(badRow.row)) *> badRow.ack
        }
    }

  def mkTypesSink[F[_]: Concurrent](project: String, topic: String, maxConcurrency: Int): Resource[F, Pipe[F, Set[ShreddedType], Unit]] =
    mkProducer[F](project, topic, 4L, 200.millis).map { p =>
      (s: Stream[F, Set[ShreddedType]]) =>
        s.parEvalMapUnordered(maxConcurrency) { types =>
          p.produce(PubSubOutput.WriteObservedTypes(types)).void
        }
    }

}
