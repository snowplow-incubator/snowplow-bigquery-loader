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
package com.snowplowanalytics.snowplow.storage.bqmutator

import io.circe.jawn.parse

import cats.syntax.either._
import cats.syntax.show._

import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver}
import com.google.pubsub.v1.PubsubMessage

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import Common._

class Listener(mutator: Mutator) extends MessageReceiver {
  def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
    val items = for {
      json <- parse(message.getData.toStringUtf8)
      invetoryItems <- json.as[List[InventoryItem]]
    } yield invetoryItems

    items match {
      case Right(Nil) =>
        consumer.ack()
      case Right(commands) =>
        commands.foreach(mutator.updateTable)
        consumer.ack()
      case Left(error) =>
        println(error.show)
    }
  }
}
