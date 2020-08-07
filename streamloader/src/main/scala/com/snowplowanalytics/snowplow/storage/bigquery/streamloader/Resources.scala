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

import cats.Applicative
import cats.effect.{Async, Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import com.permutive.pubsub.producer.{Model, PubsubProducer}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import fs2.{Pipe, Stream}
import io.circe.Json

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.StreamBadRow

class Resources[F[_]](
  val environment: Environment,
  val igluClient: Client[F, Json],
  val blocker: Blocker,
  val source: Stream[F, Payload[F]],
  val badRowsSink: Pipe[F, StreamBadRow[F], Unit],
  val typesSink: Pipe[F, Set[ShreddedType], Unit],
  val bigQuery: BigQuery,
  val failedInsertsProducer: PubsubProducer[F, Bigquery.FailedInsert]
)

object Resources {

  /**
    * Initialise and allocate resources, and clients containing cache and mutable state.
    * @param env parsed environment config
    * @tparam F effect type that can allocate Iglu cache
    * @return allocated `Resources`
    */
  def acquire[F[_]: Concurrent: ContextShift: InitSchemaCache: InitListCache](
    env: Environment
  ): Resource[F, Resources[F]] = {
    val clientF: F[Client[F, Json]] = Client.parseDefault[F](env.resolverJson).value.flatMap {
      case Right(client) =>
        Applicative[F].pure(client)
      case Left(error) =>
        Sync[F].raiseError[Client[F, Json]](new RuntimeException(s"Cannot decode Iglu Client: ${error.show}"))
    }

    // format: off
    for {
      client         <- Resource.liftF[F, Client[F, Json]](clientF)
      blocker        <- Blocker[F]
      source         = Source.getStream[F](env.config.projectId, env.config.input, blocker)
      maxConcurrency <- Resource.liftF[F, Int](Sync[F].delay(Runtime.getRuntime.availableProcessors * 8))
      badRows        <- mkBadRowsSink[F](env.config.projectId, env.config.badRows, maxConcurrency)
      types          <- mkTypesSink[F](env.config.projectId, env.config.typesTopic, maxConcurrency)
      bigquery       <- Resource.liftF[F, BigQuery](Bigquery.getClient)
      failedInserts  <- mkProducer[F, Bigquery.FailedInsert](env.config.projectId, env.config.failedInserts, 8L, 2.seconds)
    } yield new Resources[F](env, client, blocker, source, badRows, types, bigquery, failedInserts)
    // format: on
  }

  /** Construct a PubSub producer. */
  def mkProducer[F[_]: Async, A: MessageEncoder](
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
  def mkBadRowsSink[F[_]: Concurrent](
    project: String,
    topic: String,
    maxConcurrency: Int
  ): Resource[F, Pipe[F, StreamBadRow[F], Unit]] =
    mkProducer[F, BadRow](project, topic, 8L, 2.seconds).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { badRow =>
        p.produce(badRow.row) *> badRow.ack
      }
    }

  // Does not ack the event -- it still needs to end up in one of the other targets
  def mkTypesSink[F[_]: Concurrent](
    project: String,
    topic: String,
    maxConcurrency: Int
  ): Resource[F, Pipe[F, Set[ShreddedType], Unit]] =
    mkProducer[F, Set[ShreddedType]](project, topic, 4L, 200.millis).map { p =>
      _.parEvalMapUnordered(maxConcurrency) { types =>
        p.produce(types).void
      }
    }
}
