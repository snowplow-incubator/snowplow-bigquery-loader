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
import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import fs2.Stream

sealed trait Source {
  def getStream: Stream[IO, String]
}

object Source {
  // For tests
  final class StaticSource(size: Int, valid: String, invalid: String) extends Source {
    override def getStream: Stream[IO, String] =
      Stream.emit(valid).repeat.intersperse(invalid).covary[IO].take(size.toLong)
  }

  // For prod
  final class PubsubSource(env: Environment)(implicit cs: ContextShift[IO], c: Concurrent[IO]) extends Source {
    implicit val messageDecoder: MessageDecoder[String] = (bytes: Array[Byte]) => {
      Right(new String(bytes))
    }

    override def getStream: Stream[IO, String] =
      for {
        blocker <- Stream.resource(Blocker[IO])
        stream <- PubsubGoogleConsumer.subscribeAndAck[IO, String](
          blocker,
          Model.ProjectId(env.config.projectId),
          Model.Subscription(env.config.input),
          (msg, err, _, _) => IO.delay(println(s"Msg $msg got error $err")),
          config = PubsubGoogleConsumerConfig(onFailedTerminate = _ => IO.unit)
        )
      } yield stream
  }
}
