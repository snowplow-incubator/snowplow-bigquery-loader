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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.spotify.scio.Args
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.{
  Environment,
  decodeBase64Hocon,
  decodeBase64Json
}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment

/**
  * Loader specific CLI configuration.
  * Requires --key=value format and ignores unknown options (for Dataflow)
  */
object LoaderCli {
  def parse(args: Args): Either[Throwable, LoaderEnvironment] =
    for {
      c <- decodeBase64Hocon(args("config"))
      r <- decodeBase64Json(args("resolver"))
      e = Environment(c.loader, r, c.projectId)
    } yield e
}
