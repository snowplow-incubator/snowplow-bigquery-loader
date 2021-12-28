/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs._
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.MutatorEnvironment

import cats.effect.{ContextShift, IO}
import cats.syntax.either._
import cats.syntax.show._
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import fs2.concurrent.Queue
import io.circe.{Decoder, DecodingFailure, Error, Json}
import io.circe.jawn.parse

import org.typelevel.log4cats.Logger

import java.util.concurrent.TimeUnit

/**
  * PubSub consumer, listening for shredded types and
  * enqueing them into common type queue
  */
class TypeReceiver(queue: Queue[IO, List[ShreddedType]], verbose: Boolean)(implicit logger: Logger[IO]) extends MessageReceiver {
  def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
    val loggingF: IO[Unit] = if (verbose) {
      logger.info(message.getData.toStringUtf8)
    } else {
      IO.unit
    }

    val itemsF: IO[Either[Error, List[ShreddedType]]] = IO.delay {
      for {
        json <- parse(message.getData.toStringUtf8)
        invetoryItems <- TypeReceiver.decodeItems(json)
      } yield invetoryItems
    }

    val dequeueF: Either[Error, List[ShreddedType]] => IO[Unit] = {
      case Right(Nil) =>
        IO.delay(consumer.ack())
      case Right(inventoryItems) =>
        queue
          .enqueue1(inventoryItems)
          .runAsync { callback =>
            notificationCallback(consumer)(callback)
          }.toIO
      case i@Left(_) =>
        notificationCallback(consumer)(i)
    }

    val io: IO[Unit] = for {
      _ <- loggingF
      items <- itemsF
      _ <- dequeueF(items)
    } yield ()

    io.unsafeRunSync()
  }

  private def notificationCallback(consumer: AckReplyConsumer)(either: Either[Throwable, _]): IO[Unit] =
    either match {
      case Right(_) =>
        IO(consumer.ack())
      case Left(error: Error) =>
        IO(consumer.ack()) *> logger.error(error.show)
      case Left(queueError) =>
        IO(consumer.ack()) *> logger.error(queueError.getMessage)
    }
}

object TypeReceiver {
  private val UserAgent =
    FixedHeaderProvider.create("User-Agent", generated.BuildInfo.userAgent)

  /** Decode inventory items either in legacy (non-self-describing) format or as `shredded_types` schema'ed */
  def decodeItems(json: Json): Decoder.Result[List[ShreddedType]] =
    json.as[List[ShreddedType]].orElse {
      SelfDescribingData.parse(json) match {
        case Left(error) =>
          DecodingFailure(s"JSON payload is not legacy format neither self-describing, ${error.code}", Nil).asLeft
        case Right(
            SelfDescribingData(
              SchemaKey("com.snowplowanalytics.snowplow", "shredded_type", "jsonschema", SchemaVer.Full(1, _, _)),
              data
            )
            ) =>
          data.as[ShreddedType].map(item => List(item))
        case Right(SelfDescribingData(key, _)) =>
          DecodingFailure(s"JSON payload has type ${key.toSchemaUri}, which is unknown", Nil).asLeft
      }
    }

  def initQueue(size: Int)(implicit cs: ContextShift[IO]): IO[Queue[IO, List[ShreddedType]]] =
    Queue.bounded[IO, List[ShreddedType]](size)

  def apply(queue: Queue[IO, List[ShreddedType]], verbose: Boolean)(implicit logger: Logger[IO]): TypeReceiver =
    new TypeReceiver(queue, verbose)

  def startSubscription(env: MutatorEnvironment, listener: TypeReceiver): IO[Unit] =
    IO {
      val subscription = ProjectSubscriptionName.of(env.projectId, env.config.input.subscription)
      val subscriber   = Subscriber.newBuilder(subscription, listener).setHeaderProvider(UserAgent).build()
      subscriber.startAsync().awaitRunning(10L, TimeUnit.SECONDS)
    }
}
