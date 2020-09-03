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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import cats.effect._

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    StreamLoaderCli.parse(args) match {
      case Right(conf) =>
        Config.transform[IO](conf).value.flatMap {
          case Right(env) =>
            StreamLoader.run(env).as(ExitCode.Success)
          case Left(error) =>
            IO(System.err.println(s"Could not initialise Loader environment\n${error.getMessage}")).as(ExitCode.Error)
        }
      case Left(help) =>
        IO.delay(System.err.println(help.toString)).as(ExitCode.Error)
    }
}
