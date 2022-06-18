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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.{CliConfig, EncodedOrPath, Environment}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment

import cats.implicits._

import io.circe.Json

import com.typesafe.config.{Config => TypesafeConfig}

import com.spotify.scio.Args

/**
  * Loader specific CLI configuration.
  * Requires --key=value format and ignores unknown options (for Dataflow)
  */
object LoaderCli {
  def parse(args: Args): Either[Throwable, LoaderEnvironment] = {
    val result = for {
      hocon  <- EncodedOrPath.tryEncoded[TypesafeConfig](args("config"))
      json   <- EncodedOrPath.tryEncoded[Json](args("resolver"))
      parsed <- CliConfig.parseRaw(CliConfig.Raw(Some(hocon), json))
    } yield Environment(parsed.config.loader, parsed.resolver, parsed.config.projectId, parsed.config.monitoring)
    result.leftMap(e => new IllegalArgumentException(e))
  }
}
