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

import io.circe.Json

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{Config, Monitoring}

final case class Environment[A](config: A, resolverJson: Json, projectId: String, monitoring: Monitoring) {
  def getFullSubName(sub: String): String     = s"projects/$projectId/subscriptions/$sub"
  def getFullTopicName(topic: String): String = s"projects/$projectId/topics/$topic"
}

object Environment {
  type LoaderEnvironment   = Environment[Config.Loader]
  type MutatorEnvironment  = Environment[Config.Mutator]
  type RepeaterEnvironment = Environment[Config.Repeater]
}
