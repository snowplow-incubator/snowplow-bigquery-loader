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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import io.chrisdavenport.log4cats.Logger
import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment

import cats.effect._
import cats.implicits._

object StreamLoader {
  private val processor: Processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  /**
    * PubSub message with successfully parsed row, ready to be inserted into BQ.
    * Includes an `ack` action to be performed after the event is sunk.
    */
  final case class StreamLoaderRow[F[_]](row: LoaderRow, ack: F[Unit])

  /**
    * PubSub message with a row that failed parsing (a `BadRow`).
    * Includes an `ack` action to be performed after the bad row is sunk.
    */
  final case class StreamBadRow[F[_]](row: BadRow, ack: F[Unit])

  type Parsed = Either[StreamBadRow[IO], StreamLoaderRow[IO]]

  def run(e: LoaderEnvironment)(implicit CS: ContextShift[IO], C: Concurrent[IO], T: Timer[IO], logger: Logger[IO]): IO[ExitCode] =
    Resources.acquire(e).use { resources =>
      val eventStream = resources.source.evalMap(parse(resources.igluClient.resolver))

      val load = eventStream.observeEither[StreamBadRow[IO], StreamLoaderRow[IO]](
        resources.badSink,
        resources.goodSink
      )

      val metrics = resources.metrics.report

      logger.info(s"BQ Stream Loader ${generated.BuildInfo.version} has started. Listening ${e.config.input.subscription}") *>
        load.merge(metrics).compile.drain.attempt.flatMap {
          case Right(_) =>
            logger.info("Application shutting down") >> IO.pure(ExitCode.Success)
          case Left(e) =>
            logger.error(s"Application shutting down with error: $e") *> IO.raiseError(e) >> IO.pure(ExitCode.Error)
        }
    }

  /** Parse a PubSub message into a `LoaderRow` (or `BadRow`) and attach `ack` action to be used after sink. */
  def parse(igluClient: Resolver[IO])(payload: Payload[IO])(implicit C: Clock[IO]): IO[Parsed] =
    LoaderRow.parse[IO](igluClient, processor)(payload.value).map {
      case Right(row) => StreamLoaderRow[IO](row, payload.ack).asRight[StreamBadRow[IO]]
      case Left(row)  => StreamBadRow[IO](row, payload.ack).asLeft[StreamLoaderRow[IO]]
    }
}
