/*
 * Copyright (c) 2018-2023 Snowplow Analytics Ltd. All rights reserved.
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

import java.time.Instant
import java.nio.charset.StandardCharsets

import cats.implicits._

import cats.effect.{Async, Resource}

import com.permutive.pubsub.producer.{Model, PubsubProducer}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload => BadRowPayload}

import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait Producer[F[_], A] {
  def produce(data: A): F[Unit]
}

object Producer {

  val MaxPayloadLength = 9000000 // Stay under Pubsub Maximum of 10MB

  def mkProducer[F[_]: Async: Logger, A: MessageEncoder](
    projectId: String,
    topic: String,
    batchSize: Long,
    delay: FiniteDuration,
    oversizeBadRowProducer: Producer[F, BadRow.SizeViolation]
  ): Resource[F, Producer[F, A]] =
    mkPubsubProducer[F, A](projectId, topic, batchSize, delay).map { p =>
      (data: A) => produceWrtToSize[F, A](data, p.produce(_).void, oversizeBadRowProducer)
    }

  def mkBadRowProducers[F[_]: Async: Logger](
    projectId: String,
    topic: String,
    batchSize: Long,
    delay: FiniteDuration
  ): Resource[F, (Producer[F, BadRow], Producer[F, BadRow.SizeViolation])] =
    mkPubsubProducer[F, BadRow](projectId, topic, batchSize, delay).map { p =>
      val badRowProducer = new Producer[F, BadRow] {
        override def produce(badRow: BadRow): F[Unit] = {
          produceWrtToSize[F, BadRow](badRow, p.produce(_).void, p.produce(_).void)
        }
      }
      val oversizeBadRowProducer = new Producer[F, BadRow.SizeViolation] {
        override def produce(data: BadRow.SizeViolation): F[Unit] =
          p.produce(data).void
      }
      (badRowProducer, oversizeBadRowProducer)
    }

  /** Construct a PubSub producer. */
  private def mkPubsubProducer[F[_]: Async: Logger, A: MessageEncoder](
    projectId: String,
    topic: String,
    batchSize: Long,
    delay: FiniteDuration
  ): Resource[F, PubsubProducer[F, A]] =
    GooglePubsubProducer.of[F, A](
      Model.ProjectId(projectId),
      Model.Topic(topic),
      config = PubsubProducerConfig[F](
        batchSize = batchSize,
        delayThreshold = delay,
        onFailedTerminate = e => Logger[F].error(e)(s"Error in PubSub producer")
      )
    )

  def produceWrtToSize[F[_]: Async, A: MessageEncoder](
    data: A,
    producer: Producer[F, A],
    oversizeBadRowProducer: Producer[F, BadRow.SizeViolation]
  ): F[Unit] = {
    val dataSize = getSize(data)
    if (dataSize >= MaxPayloadLength) {
      val msg = s"Pubsub message exceeds allowed size"
      val payload = MessageEncoder[A].encode(data)
        .map(bytes => new String(bytes, StandardCharsets.UTF_8))
        .getOrElse("Pubsub message can't be converted to string")
        .take(MaxPayloadLength / 10)
      val badRow = BadRow.SizeViolation(
          processor,
          Failure.SizeViolation(Instant.now(), MaxPayloadLength, dataSize, msg),
          BadRowPayload.RawPayload(payload)
        )
      oversizeBadRowProducer.produce(badRow)
    } else {
      producer.produce(data).void
    }
  }

  private def getSize[A: MessageEncoder](a: A): Int =
    MessageEncoder[A].encode(a).map(_.length).getOrElse(Int.MaxValue)
}
