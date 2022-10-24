/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.storage.bigquery.common.{LoaderRow, LookupProperties}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment

import cats.Monad
import cats.effect._
import cats.implicits._

import fs2.Pipe

import org.typelevel.log4cats.Logger

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

  type Parsed[F[_]] = Either[StreamBadRow[F], StreamLoaderRow[F]]

  def run[F[_]: Async: Logger](e: LoaderEnvironment): F[ExitCode] =
    Resources.acquire(e).use { resources =>
      implicit val rl: RegistryLookup[F] = resources.registryLookup
      val eventStream                    = resources.source.evalMap(parse(resources.resolver, resources.lookup))

      val sink: Pipe[F, Parsed[F], Nothing] = _.observeEither(
        resources.badSink,
        resources.goodSink
      ).drain

      val metrics = resources.metrics.report

      Logger[F].info(
        s"BQ Stream Loader ${generated.BuildInfo.version} has started. Listening ${e.config.input.subscription}"
      ) *>
        Shutdown
          .run(e.config.terminationTimeout, resources.sentry, eventStream.concurrently(metrics), sink)
          .compile
          .drain
          .attempt
          .flatMap {
            case Right(_) =>
              Logger[F].info("Application shutting down") >> ExitCode.Success.pure[F]
            case Left(e) =>
              Logger[F].error(e)(s"Application shutting down with error") *>
                ExitCode.Error.pure[F]
          }
    }

  /** Parse a PubSub message into a `LoaderRow` (or `BadRow`) and attach `ack` action to be used after sink. */
  def parse[F[_]: Clock: Monad: RegistryLookup](igluClient: Resolver[F], lookup: LookupProperties[F])(payload: Payload[F]): F[Parsed[F]] =
    LoaderRow.parse[F](igluClient, processor, lookup)(payload.value).map {
      case Right(row) => StreamLoaderRow[F](row, payload.ack).asRight[StreamBadRow[F]]
      case Left(row)  => StreamBadRow[F](row, payload.ack).asLeft[StreamLoaderRow[F]]
    }

}
