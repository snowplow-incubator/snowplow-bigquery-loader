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
import cats.effect.{Async, Resource, Sync, Blocker}

import io.circe.Json

import com.google.cloud.bigquery.BigQuery

import com.permutive.pubsub.producer.Model
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.{Bigquery, PubSub}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.PubSub.{Producer, PubSubOutput}

class Resources[F[_]](val pubsub: PubSub.Producer[F],
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
  def acquire[F[_]: Async: InitSchemaCache: InitListCache](env: Environment): Resource[F, Resources[F]] = {
    val clientF: F[Client[F, Json]] = Client.parseDefault[F](env.resolverJson).value.flatMap {
      case Right(client) =>
        Applicative[F].pure(client)
      case Left(error) =>
        Sync[F].raiseError[Client[F, Json]](new RuntimeException(s"Cannot decode Iglu Client: ${error.show}"))
    }

    for {
      pubsub <- mkProducer[F](env.config.projectId, env.config.failedInserts)
      bigquery <- Resource.liftF(Bigquery.getClient)
      client <- Resource.liftF(clientF)
      blocker <- Blocker[F]
    } yield new Resources[F](pubsub, bigquery, client, env, blocker)
  }

  /** Constructor for PubSub producer (reader) */
  def mkProducer[F[_]: Async](projectId: String, topic: String): Resource[F, Producer[F]] =
    GooglePubsubProducer
      .of[F, PubSubOutput](
        Model.ProjectId(projectId),
        Model.Topic(topic),
        config = PubsubProducerConfig[F](
          // TODO: Get rid of magic numbers
          batchSize         = 100,
          delayThreshold    = 100.millis,
          onFailedTerminate = e => Sync[F].delay(println(s"Got error $e")).void
        )
      )
}
