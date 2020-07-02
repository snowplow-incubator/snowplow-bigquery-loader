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

import cats.effect.{Concurrent, ExitCode, IO, Resource}
import cats.syntax.all._
import fs2.Stream

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.permutive.pubsub.producer.Model
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow
import io.circe.syntax._
import io.circe._
import io.circe.generic.semiauto._
import com.google.api.client.json.jackson2.JacksonFactory

object PubSub {
  sealed trait PubSubOutput
  final case class WriteBadRow(badRow: BadRow) extends PubSubOutput
  final case class WriteTableRow(tableRow: String) extends PubSubOutput
  final case class WriteObservedTypes(types: Set[ShreddedType]) extends PubSubOutput

  implicit val messageEncoder: MessageEncoder[PubSubOutput] = new MessageEncoder[PubSubOutput] {
    override def encode(a: PubSubOutput): Either[Throwable, Array[Byte]] =
      // This get's evaluated last, when producer.produce(r) is called
      a match {
        case WriteBadRow(br)       => val b  = Right(br.compact.getBytes()); println(b); b              // works
        case WriteTableRow(tr)     => val g  = Right(tr.getBytes()); println(g); g                      // does not work
        case WriteObservedTypes(t) => val ty = Right(toPayload(t).noSpaces.getBytes()); println(ty); ty // works
      }
  }

  def sink(projectId: String, topic: String)(r: PubSubOutput): IO[Unit] = {
    // To show that sink gets called with the correct type of r
    r match {
      case WriteBadRow(badRow)       => println("Bad")
      case WriteTableRow(tableRow)   => println("Good")
      case WriteObservedTypes(types) => println("Types")
    }

    val res = GooglePubsubProducer.of[IO, PubSubOutput](
      Model.ProjectId(projectId),
      Model.Topic(topic),
      config = PubsubProducerConfig[IO](
        batchSize         = 100,
        delayThreshold    = 100.millis,
        onFailedTerminate = e => IO(println(s"Got error $e")) >> IO.unit
      )
    )

    // To show that the resource gets allocated and the thing we want to sink is the expected thing
    res match {
      case Resource.Allocate(resource) => println(s"Allocate $resource for $r")
      case b: Resource.Bind            => println(s"Bind $b for $r")
      case Resource.Suspend(resource)  => println(s"Suspend for $r")
    }

    res
      .use[Model.MessageId] { producer =>
        println(s"Producer: $producer") // Does not get created for WriteLoaderRow
        r match {
          case WriteTableRow(tr)     => println(s"No producer for: $tr") // Never gets called although it's expected to
          case WriteBadRow(_)        => println("Bad.")                  // Gets called when expected
          case WriteObservedTypes(_) => println("Types.")                // Gets called when expected
          case _                     => println(s"Not a loader row: $r") // Never gets called and is not expected to
        }
        producer.produce(r)
      }
      .map(_ => ())
  }
}
