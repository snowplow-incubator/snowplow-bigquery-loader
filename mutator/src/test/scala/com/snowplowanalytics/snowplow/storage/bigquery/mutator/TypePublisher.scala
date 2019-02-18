/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery
package mutator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import io.circe.syntax._

import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage
import com.google.protobuf.ByteString

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaVer }

import fs2._

import cats.effect.{IO, Resource}

import common.Codecs._

/**
  * Example publisher that submits Inventory items to specified topic.
  * Mutator can be launched from `Main`
  *
  * {{{
  *   sbt:snowplow-bigquery-mutator> test:console
  *   scala> import com.snowplowanalytics.snowplow.storage.bqmutator.TypePublisher
  *   scala> TypePublisher.run("projects/engineering-sandbox/topics/bq-test-types", TypePublisher.inventoryItems)
  * }}}
  */
object TypePublisher {

  implicit val cs = IO.contextShift(global)
  implicit val timer = IO.timer(global)

  val inventoryItems = List(
    ShreddedType(Contexts(CustomContexts), SchemaKey("com.mparticle.snowplow","appstatetransition_event","jsonschema",SchemaVer.Full(1,0,0))),
    ShreddedType(Contexts(DerivedContexts),SchemaKey("com.snowplowanalytics.snowplow","application_background","jsonschema",SchemaVer.Full(1,0,0))),
    ShreddedType(Contexts(CustomContexts), SchemaKey("com.snowplowanalytics.snowplow","ad_click","jsonschema",SchemaVer.Full(1,0,0))),
    ShreddedType(Contexts(CustomContexts), SchemaKey("com.snowplowanalytics.snowplow","ua_parser_context","jsonschema",SchemaVer.Full(1,0,0))),
    ShreddedType(UnstructEvent, SchemaKey("com.snowplowanalytics.snowplow","payload_data","jsonschema",SchemaVer.Full(1,0,3)))
  )

  def run(topic: String, items: List[ShreddedType]): Unit = {
    val bracket =
      Stream.resource(Resource.make(newPublisher(topic))(shutdown))

    val appStream = for {
      publisher <- bracket
      item <- Stream.emits(items)
      _ <- Stream.eval(publish(publisher, item))
    } yield ()

    val ticks = Stream.awakeEvery[IO](5.seconds)
    ticks.zip(appStream).compile.drain.unsafeRunSync()
  }

  private def publish(publisher: Publisher, inventoryItem: ShreddedType): IO[Unit] = {
    val message = inventoryToMessage(inventoryItem)
    IO(println(s"Publishing ${inventoryItem.schemaKey.toSchemaUri}")).flatMap(_ => IO(publisher.publish(message)))
  }

  private def newPublisher(topic: String): IO[Publisher] =
    IO(Publisher.newBuilder(topic).build())

  private def shutdown(p: Publisher): IO[Unit] =
    IO(p.shutdown())

  private def inventoryToMessage(inventoryItem: ShreddedType): PubsubMessage =
    PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(List(inventoryItem).asJson.noSpaces)).build()
}