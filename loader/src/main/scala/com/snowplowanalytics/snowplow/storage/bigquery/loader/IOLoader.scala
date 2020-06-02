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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import cats.effect.{Concurrent, ContextShift, ExitCode, IO}
import cats.syntax.all._
import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import fs2.{Chunk, Stream}
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.loader.poc.events._
import com.snowplowanalytics.snowplow.storage.bigquery.loader.sinks.PubSub
import com.snowplowanalytics.snowplow.storage.bigquery.loader.sinks.PubSub.{WriteBadRow, WriteObservedTypes}
import org.slf4j.LoggerFactory

object IOLoader {
  private val MaxConcurrency        = 5
  implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)
  implicit val messageDecoder: MessageDecoder[String] = (bytes: Array[Byte]) => {
    Right(new String(bytes))
  }

  def run(env: Environment): IO[ExitCode] = {
    //val rawData: Stream[IO, String] = Stream.emit(valid.event).repeat.intersperse(invalid.event).covary[IO].take(10)
    val rawStream = PubsubGoogleConsumer.subscribeAndAck[IO, String](
      Model.ProjectId(env.config.projectId),
      Model.Subscription(env.config.input),
      (msg, err, _, _) => IO(println(s"Msg $msg got error $err")),
      config = PubsubGoogleConsumerConfig(onFailedTerminate = _ => IO.unit)
    )
    val parsedData: Stream[IO, Either[BadRow, LoaderRow]] =
      rawStream.map(LoaderRow.parse(env.resolverJson))

    def sink(s: String)(name: String): Unit = LoggerFactory.getLogger(name).info(s)

    /*val good: Stream[IO, Unit] = parsedData.parEvalMapUnordered(MaxConcurrency) {
      case Right(loaderRow) => IO(sink(loaderRow.toString)("good"))
      case _                => IO(())
    }

    val types: Stream[IO, Unit] = parsedData.parEvalMapUnordered(MaxConcurrency) {
      case Right(loaderRow) =>
        PubSub.insert(env.config.projectId, env.config.typesTopic)(WriteObservedTypes(loaderRow.inventory))
      case _ => IO(())
    }

    val bad: Stream[IO, Unit] = parsedData.parEvalMapUnordered(MaxConcurrency) {
      case Left(badRow) => IO(sink(badRow.toString)("bad"))
      case _            => IO(())
    }

    Stream(good, types, bad).parJoin(3).compile.drain.as(ExitCode.Success)
     */

    def aggregateTypes(types: Stream[IO, Set[ShreddedType]]): Stream[IO, Set[ShreddedType]] =
      types.mapChunks(c => Chunk(c.foldLeft[Set[ShreddedType]](Set.empty[ShreddedType])(_ ++ _)))

    parsedData.observeEither[BadRow, LoaderRow](
      badRows =>
        badRows.parEvalMapUnordered(MaxConcurrency) { badRow =>
          PubSub.sink(env.config.projectId, env.config.badRows)(WriteBadRow(badRow))
        },
      loaderRows =>
        aggregateTypes(loaderRows.map(loaderRow => loaderRow.inventory)).parEvalMapUnordered(MaxConcurrency) {
          aggregate =>
            PubSub.sink(env.config.projectId, env.config.typesTopic)(WriteObservedTypes(aggregate))
        } *>
          loaderRows.parEvalMapUnordered(MaxConcurrency) { loaderRow =>
            IO(sink(loaderRow.toString)("good"))
          }
    )
  }.compile.drain.as(ExitCode.Success)
}
