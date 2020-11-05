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
import com.permutive.pubsub.consumer.ConsumerRecord
import io.chrisdavenport.log4cats.Logger
import fs2.concurrent.Queue
import fs2.Stream

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{EventContainer, Repeater}
import com.permutive.pubsub.producer.grpc.GooglePubsubProducer
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.consumer
import com.permutive.pubsub.producer
import com.permutive.pubsub.producer.grpc.PubsubProducerConfig
import scala.concurrent.duration._

/** Module responsible for reading and writing PubSub */
object PubSub {

  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: ContextShift: Concurrent: Timer: Logger](
    blocker: Blocker,
    projectId: String,
    subscription: String,
    desperates: Queue[F, BadRow]
  ): Stream[F, ConsumerRecord[F, EventContainer]] =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      blocker,
      consumer.Model.ProjectId(projectId),
      consumer.Model.Subscription(subscription),
      (msg, err, ack, _) => callback[F](msg, err, ack, desperates),
      PubsubGoogleConsumerConfig[F](onFailedTerminate = t => Logger[F].error(s"Terminating consumer due $t"))
    )

  private def callback[F[_]: Sync](msg: PubsubMessage, err: Throwable, ack: F[Unit], desperates: Queue[F, BadRow]) = {
    val info    = FailureDetails.LoaderRecoveryError.ParsingError(err.toString, Nil)
    val failure = Failure.LoaderRecoveryFailure(info)
    val badRow  = BadRow.LoaderRecoveryError(Repeater.processor, failure, Payload.RawPayload(msg.toString))
    desperates.enqueue1(badRow) >> ack
  }

  implicit val encoder: MessageEncoder[EventContainer] = new MessageEncoder[EventContainer] {
    override def encode(a: EventContainer): Either[Throwable, Array[Byte]] = ???
  }

  //TODO: provide projectId and topic
  def getProducer[F[_]: Concurrent: Timer: Logger] =
    GooglePubsubProducer.of[F, EventContainer](
      producer.Model.ProjectId("test-project"),
      producer.Model.Topic("values"),
      config = PubsubProducerConfig[F](
        batchSize         = 100,
        delayThreshold    = 100.millis,
        onFailedTerminate = e => Sync[F].pure(println(s"Got error $e")) >> Sync[F].unit
      )
    )

}
