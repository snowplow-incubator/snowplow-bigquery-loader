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

import scala.collection.mutable.ListBuffer
import scala.util.Random
import java.nio.charset.StandardCharsets
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Failure}
import org.specs2.mutable.Specification

class ProducerSpec extends Specification {

  implicit val runtime: IORuntime = IORuntime.global

  implicit val stringEncoder: MessageEncoder[String] = { str =>
    Right(str.getBytes(StandardCharsets.UTF_8))
  }

  class MockProducer[A] extends Producer[IO, A] {
    val buf: ListBuffer[A] = ListBuffer.empty
    override def produce(data: A): IO[Unit] = IO.delay(buf.addOne(data))
    def get: List[A] = buf.toList
  }

  "produceWrtToSize" should {
    "send normal size messages to normal producer" in {
      val producer = new MockProducer[String]
      val oversizeBadRowProducer = new MockProducer[BadRow.SizeViolation]

      val messages = (1 to 100).map(i => s"test-$i")

      messages.foreach(
        Producer.produceWrtToSize(_, producer, oversizeBadRowProducer).unsafeRunSync()
      )

      producer.get must beEqualTo(messages)
      oversizeBadRowProducer.get must beEmpty
    }

    "send oversize messages to oversize bad row producer" in {
      val producer = new MockProducer[String]
      val oversizeBadRowProducer = new MockProducer[BadRow.SizeViolation]

      val messageSize = Producer.MaxPayloadLength + 1
      val message = Random.alphanumeric.take(messageSize).mkString

      Producer.produceWrtToSize(message, producer, oversizeBadRowProducer).unsafeRunSync()

      val expectedBadRowPayload = message.take(Producer.MaxPayloadLength / 10)

      producer.get must beEmpty
      oversizeBadRowProducer.get must beLike {
        case List(BadRow.SizeViolation(
          `processor`,
          Failure.SizeViolation(_, Producer.MaxPayloadLength, `messageSize`, _),
          Payload.RawPayload(`expectedBadRowPayload`)
        )) => ok
      }
    }
  }

}
