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

import cats.effect.{Blocker, Concurrent, ContextShift, IO}
import fs2.Stream
import com.permutive.pubsub.consumer.{ConsumerRecord, Model}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Source.Payload

import scala.concurrent.duration.FiniteDuration

sealed trait Source {
  def getStream(implicit cs: ContextShift[IO], c: Concurrent[IO]): Stream[IO, Payload]
}

object Source {

  type Payload = ConsumerRecord[IO, String]

  // For tests
  final class StaticSource(size: Int, valid: String, invalid: String) extends Source {
    override def getStream(implicit cs: ContextShift[IO], c: Concurrent[IO]): Stream[IO, Payload] = {
      val validRecord: Payload = new ConsumerRecord[IO, String] {
        def value: String                                = valid
        def ack: IO[Unit]                                = IO.unit
        def nack: IO[Unit]                               = IO.unit
        def extendDeadline(by: FiniteDuration): IO[Unit] = IO.unit
      }

      val invalidRecord = new ConsumerRecord[IO, String] {
        def value: String                                = invalid
        def ack: IO[Unit]                                = IO.unit
        def nack: IO[Unit]                               = IO.unit
        def extendDeadline(by: FiniteDuration): IO[Unit] = IO.unit
      }

      Stream.emit(validRecord).repeat.intersperse(invalidRecord).covary[IO].take(size.toLong)
    }
  }

  // For prod
  final class PubsubSource(env: Environment) extends Source {
    implicit val messageDecoder: MessageDecoder[String] = (bytes: Array[Byte]) => {
      Right(new String(bytes))
    }

    override def getStream(implicit cs: ContextShift[IO], c: Concurrent[IO]): Stream[IO, Payload] =
      for {
        blocker <- Stream.resource(Blocker[IO])
        stream <- PubsubGoogleConsumer.subscribe[IO, String](
          blocker,
          Model.ProjectId(env.config.projectId),
          Model.Subscription(env.config.input),
          (msg, err, _, _) => IO.delay(System.err.println(s"Msg $msg got error $err")),
          config = PubsubGoogleConsumerConfig(onFailedTerminate = _ => IO.unit)
        )
      } yield stream
  }
}
