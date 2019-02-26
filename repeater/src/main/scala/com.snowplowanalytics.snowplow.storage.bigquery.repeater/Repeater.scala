/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import scala.concurrent.duration._

import cats.syntax.all._
import cats.effect._

import fs2.Stream

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config

import scala.util.control.NonFatal

object Repeater extends SafeIOApp {

  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] = {
    RepeaterCli.parse(args) match {
      case Right(command) =>
        val process = for {
          resources <- Stream.resource(Resources.acquire[IO](command))
          _         <- Stream.eval(resources.showStats)
          events     = services.PubSub.getEvents(resources.env.config.projectId, command.failedInsertsSub).interruptWhen(resources.stop)
          processing = Flow.process(resources)(events)
          logging    = Stream.awakeEvery[IO](5.minute).evalMap(_ => resources.showStats)
          _         <- processing.concurrently(logging)
        } yield ()

        process
          .compile
          .drain
          .attempt
          .flatMap {
            case Right(_) =>
              IO.delay(println("Closing Snowplow BigQuery Repeater")) *> IO.pure(ExitCode.Success)
            case Left(Config.InitializationError(message)) =>
              IO(System.err.println(s"Snowplow BigQuery Repeater failed to start. $message")) >> IO.pure(ExitCode.Error)
            case Left(e: java.util.concurrent.TimeoutException) =>
              System.out.println(e.toString)
              IO.raiseError(e) >> IO.pure(ExitCode.Error)
            case Left(NonFatal(e)) =>
              System.out.println(e.toString)
              IO.raiseError(e) >> IO.pure(ExitCode.Error)
          }
      case Left(error) =>
        IO(println(error.toString())) >> IO.pure(ExitCode.Error)
    }
  }
}
