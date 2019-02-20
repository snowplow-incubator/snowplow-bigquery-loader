/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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

import cats.syntax.either._

import com.spotify.scio.Args

import common.Config._

/**
  * Loader specific CLI configuration
  * Unlike Mutator, required --key=value format and ignores unknown options (for Dataflow)
  */
object LoaderCli {
  def parse(args: Args): Either[Throwable, Environment] =
    for {
      c <- decodeBase64Json(args("config"))
      r <- decodeBase64Json(args("resolver"))
      e <- transform(EnvironmentConfig(r, c)).value.unsafeRunSync()
    } yield e
}

