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
package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader.sinks

import cats.effect.{Concurrent, ExitCode, IO}
import cats.syntax.all._
import fs2.Stream

import scala.concurrent.duration._
import com.permutive.pubsub.producer.Model
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload

object PubSub {
  sealed trait PubSubOutput
  final case class WriteBadRow(badRow: BadRow) extends PubSubOutput
  final case class WriteObservedTypes(types: Set[ShreddedType]) extends PubSubOutput

  implicit val messageEncoder: MessageEncoder[PubSubOutput] = new MessageEncoder[PubSubOutput] {
    override def encode(a: PubSubOutput): Either[Throwable, Array[Byte]] =
      a match {
        case WriteBadRow(br)       => Right(br.compact.getBytes())
        case WriteObservedTypes(t) => Right(toPayload(t).noSpaces.getBytes())
      }
  }

  def sink(projectId: String, topic: String)(r: PubSubOutput): IO[Unit] =
    GooglePubsubProducer
      .of[IO, PubSubOutput](
        Model.ProjectId(projectId),
        Model.Topic(topic),
        config = PubsubProducerConfig[IO](
          batchSize         = 100,
          delayThreshold    = 100.millis,
          onFailedTerminate = e => IO(println(s"Got error $e")) >> IO.unit
        )
      )
      .use { producer =>
        producer.produce(r)
      }
      .map(_ => ())
}
