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

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.{CliConfig, Environment}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment

import cats.implicits._
import com.monovore.decline._

object StreamLoaderCli {

  val command: Command[CliConfig.Parsed] =
    Command(generated.BuildInfo.name, generated.BuildInfo.description)(CliConfig.options)

  def parse(args: Seq[String]): Either[String, LoaderEnvironment] =
    for {
      parsed <- command.parse(args).leftMap(_.show)
    } yield Environment(parsed.config.loader, parsed.resolver, parsed.config.projectId, parsed.config.monitoring)
}
