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

import cats.effect.{Clock, IO}
import cats.implicits._
import com.monovore.decline._

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config._

object StreamLoaderCli {
  implicit private val privateIoClock: Clock[IO] =
    Clock.create[IO]

  private val options = (resolverOpt, configOpt).mapN { (resolver, config) =>
    EnvironmentConfig(resolver, config)
  }

  val command = Command(generated.BuildInfo.name, generated.BuildInfo.description)(options)

  def parse(args: Seq[String]): Either[Help, EnvironmentConfig] = command.parse(args)

  def getEnv(config: EnvironmentConfig): Environment =
    transform[IO](config).value.flatMap(IO.fromEither[Environment]).unsafeRunSync()
}
