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

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model._

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

final case class AllAppsConfig(
  projectId: String,
  loader: Config.Loader,
  mutator: Config.Mutator,
  repeater: Config.Repeater,
  monitoring: Monitoring
)

object AllAppsConfig {

  implicit val allAppsConfigDecoder: Decoder[AllAppsConfig] = deriveDecoder[AllAppsConfig]
  implicit val allAppsConfigEncoder: Encoder[AllAppsConfig] = deriveEncoder[AllAppsConfig]

}
