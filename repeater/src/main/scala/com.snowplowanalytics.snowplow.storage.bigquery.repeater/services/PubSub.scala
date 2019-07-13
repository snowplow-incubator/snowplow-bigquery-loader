/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import scala.concurrent.duration._

import cats.syntax.all._
import cats.effect._

import io.chrisdavenport.log4cats.Logger

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.BadRow.PubSubError
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.Resources

/** Module responsible for reading PubSub */
object PubSub {
  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: ContextShift: Concurrent: Timer: Logger](subscription: String, resources: Resources[F], maxAckDeadlinePeriod: Int) =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      Model.ProjectId(resources.env.config.projectId),
      Model.Subscription(subscription),
      (msg, err, ack, _) => resources.logAbandoned *> resources.desperates.enqueue1(PubSubError(msg.toString, err.toString)) >> ack,
      PubsubGoogleConsumerConfig[F](
        onFailedTerminate = t => Logger[F].error(s"Terminating consumer due $t"),
        maxAckExtensionPeriod = maxAckDeadlinePeriod.second
      )
    )
}
