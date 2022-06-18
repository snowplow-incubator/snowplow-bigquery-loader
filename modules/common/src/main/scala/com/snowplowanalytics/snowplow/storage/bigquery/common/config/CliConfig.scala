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
package com.snowplowanalytics.snowplow.storage.bigquery.common.config

import cats.implicits._

import io.circe.Json

import com.monovore.decline.Opts

final case class CliConfig(
  config: Option[EncodedHoconOrPath],
  resolver: EncodedJsonOrPath
)

object CliConfig {

  /** CLI option to parse base64-encoded resolver into JSON */
  val resolverOpt: Opts[EncodedJsonOrPath] =
    Opts.option[EncodedJsonOrPath]("resolver", "Iglu Resolver configuration (self-describing json)")

  /** CLI option to parse base64-encoded config hocon */
  val configOpt: Opts[Option[EncodedHoconOrPath]] =
    Opts.option[EncodedHoconOrPath]("config", "App configuration (hocon).").orNone

  /** CLI option to parse and resolve all arguments */
  val options: Opts[(Json, AllAppsConfig)] =
    (configOpt, resolverOpt).mapN(CliConfig(_, _)).mapValidated { cli =>
      AllAppsConfig.fromRaw(cli).toValidatedNel
    }

}
