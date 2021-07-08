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

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{EventContainer, Repeater}

import cats.effect._
import cats.syntax.all._
import com.google.pubsub.v1.PubsubMessage
import com.permutive.pubsub.consumer.{ConsumerRecord, Model}
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import fs2.concurrent.Queue
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

/** Module responsible for reading Pub/Sub */
object PubSub {

  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: ContextShift: Concurrent: Timer: Logger](
    blocker: Blocker,
    projectId: String,
    subscription: String,
    uninsertable: Queue[F, BadRow]
  ): Stream[F, ConsumerRecord[F, EventContainer]] =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      blocker,
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      (msg, err, ack, _) => callback[F](msg, err, ack, uninsertable),
      PubsubGoogleConsumerConfig[F](onFailedTerminate = t => Logger[F].error(s"Terminating consumer due to $t"))
    )

  private def callback[F[_]: Sync](msg: PubsubMessage, err: Throwable, ack: F[Unit], uninsertable: Queue[F, BadRow]) = {
    val info    = FailureDetails.LoaderRecoveryError.ParsingError(err.toString, Nil)
    val failure = Failure.LoaderRecoveryFailure(info)
    val badRow  = BadRow.LoaderRecoveryError(Repeater.processor, failure, Payload.RawPayload(msg.toString))
    uninsertable.enqueue1(badRow) >> ack
  }
}
