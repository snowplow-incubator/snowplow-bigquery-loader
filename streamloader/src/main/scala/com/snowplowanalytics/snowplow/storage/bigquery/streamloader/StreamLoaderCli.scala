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

import cats.effect.IO
import cats.implicits._
import com.monovore.decline._

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig._

object StreamLoaderCli {
  private val options: Opts[LoaderEnvironment] = (configOpt, resolverOpt).mapN { (config, resolver) =>
    validateResolverJson[IO](resolver).value.unsafeRunSync()
    LoaderEnvironment(config.loader, resolver, config.projectId)
  }

  val command: Command[LoaderEnvironment] = Command(generated.BuildInfo.name, generated.BuildInfo.description)(options)

  def parse(args: Seq[String]): Either[Help, LoaderEnvironment] = command.parse(args)
}
