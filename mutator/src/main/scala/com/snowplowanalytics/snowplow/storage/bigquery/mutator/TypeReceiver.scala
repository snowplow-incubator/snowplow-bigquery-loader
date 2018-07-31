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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import scala.concurrent.{ExecutionContext, Future}
import io.circe.Error
import io.circe.jawn.parse
import cats.effect.IO
import cats.syntax.either._
import cats.syntax.show._
import cats.syntax.apply._
import fs2.async
import fs2.async.mutable.Queue
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs._

/**
  * PubSub consumer, listening for shredded types and
  * enqueing them into common type queue
  */
class TypeReceiver(queue: Queue[IO, List[InventoryItem]])
                  (implicit ec: ExecutionContext) extends MessageReceiver {

  def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
    val items: Either[Error, List[InventoryItem]] = for {
      json <- parse(message.getData.toStringUtf8)
      invetoryItems <- json.as[List[InventoryItem]]
    } yield invetoryItems

    items match {
      case Right(Nil) =>
        consumer.ack()
      case Right(inventoryItems) =>
        async.unsafeRunAsync(queue.enqueue1(inventoryItems))(notificationCallback(consumer))
      case Left(_) =>
        notificationCallback(consumer)(items).unsafeRunSync()
    }
  }

  private def notificationCallback(consumer: AckReplyConsumer)(either: Either[Throwable, _]): IO[Unit] =
    either match {
      case Right(_) => IO(consumer.ack())
      case Left(error: Error) => IO(consumer.ack()) *> IO(println(error.show))
      case Left(queueError) => IO(println(queueError.getMessage))
    }
}

object TypeReceiver {
  def initQueue(size: Int)(implicit ec: ExecutionContext): IO[Queue[IO, List[InventoryItem]]] =
    Queue.bounded[IO, List[InventoryItem]](size)

  def apply(queue: Queue[IO, List[InventoryItem]])(implicit ec: ExecutionContext): TypeReceiver =
    new TypeReceiver(queue)

  def startSubscription(config: Config, listener: TypeReceiver)(implicit ec: ExecutionContext): IO[Unit] = {
    def process = IO {
      Future {
        val subscription = ProjectSubscriptionName.of(config.projectId, config.typesSub)
        val subscriber = Subscriber.newBuilder(subscription, listener).build()
        subscriber.startAsync().awaitRunning()
      }
    }

    IO.fromFuture(process)
  }
}
