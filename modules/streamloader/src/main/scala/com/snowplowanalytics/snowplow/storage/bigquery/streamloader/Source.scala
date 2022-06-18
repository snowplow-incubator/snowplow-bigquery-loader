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

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.ConsumerSettings

import org.typelevel.log4cats.Logger

import cats.effect.Sync
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.Model
import fs2.Stream

object Source {
  def getStream[F[_]: Sync: Logger](
    projectId: String,
    subscription: String,
    cs: ConsumerSettings
  ): Stream[F, Payload[F]] =
    PubsubGoogleConsumer.subscribe[F, String](
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      (msg, err, _, _) => Logger[F].error(err)(s"Error from PubSub message $msg"),
      config = PubsubGoogleConsumerConfig(
        cs.maxQueueSize,
        cs.parallelPullCount,
        cs.maxAckExtensionPeriod,
        cs.awaitTerminatePeriod,
        onFailedTerminate = _ => Sync[F].unit
      )
    )
}
