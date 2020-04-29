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

import com.google.pubsub.v1.PubsubMessage

import cats.effect._
import cats.syntax.all._
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.Model
import io.chrisdavenport.log4cats.Logger
import fs2.concurrent.Queue
import fs2.Stream

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{EventContainer, Repeater}

/** Module responsible for reading PubSub */
object PubSub {

  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: ContextShift: Concurrent: Timer: Logger](
    projectId: String,
    subscription: String,
    desperates: Queue[F, BadRow]
  ): Stream[F, Model.Record[F, EventContainer]] =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      (msg, err, ack, _) => callback[F](msg, err, ack, desperates),
      PubsubGoogleConsumerConfig[F](onFailedTerminate = t => Logger[F].error(s"Terminating consumer due $t"))
    )

  private def callback[F[_]: Sync](msg: PubsubMessage, err: Throwable, ack: F[Unit], desperates: Queue[F, BadRow]) = {
    val info    = FailureDetails.LoaderRecoveryError.ParsingError(err.toString, Nil)
    val failure = Failure.LoaderRecoveryFailure(info)
    val badRow  = BadRow.LoaderRecoveryError(Repeater.processor, failure, Payload.RawPayload(msg.toString))
    desperates.enqueue1(badRow) >> ack
  }
}
