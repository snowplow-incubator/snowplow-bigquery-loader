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
import com.google.api.gax.batching.FlowController.LimitExceededBehavior
import com.google.api.gax.batching.FlowControlSettings
import com.google.pubsub.v1.PubsubMessage
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.Model
import fs2.Stream

object Source {
  def getStream[F[_]: Sync: Logger](
    projectId: String,
    subscription: String,
    cs: ConsumerSettings
  ): Stream[F, Payload[F]] = {

    val onFailedTerminate: Throwable => F[Unit] =
      t => Logger[F].error(s"Failed to terminate PubSub consumer: ${t.getMessage} ")

    val flowControlSettings: FlowControlSettings =
      FlowControlSettings
        .newBuilder()
        .setMaxOutstandingElementCount(cs.maxQueueSize)
        .setMaxOutstandingRequestBytes(cs.maxRequestBytes)
        .setLimitExceededBehavior(LimitExceededBehavior.Block)
        .build()

    val pubSubConfig =
      PubsubGoogleConsumerConfig(
        maxQueueSize = cs.maxQueueSize,
        parallelPullCount = cs.parallelPullCount,
        maxAckExtensionPeriod = cs.maxAckExtensionPeriod,
        awaitTerminatePeriod = cs.awaitTerminatePeriod,
        onFailedTerminate = onFailedTerminate,
        customizeSubscriber = Some(builder => builder.setFlowControlSettings(flowControlSettings))
      )

    val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] =
      (message, error, _, _) =>
        Logger[F].error(error)(s"Failed to decode message: $message")

    PubsubGoogleConsumer.subscribe[F, String](
      Model.ProjectId(projectId), Model.Subscription(subscription), errorHandler, pubSubConfig
    )
  }
}
