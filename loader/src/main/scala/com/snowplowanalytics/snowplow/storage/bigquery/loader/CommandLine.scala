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

import org.apache.beam.sdk.options._
import org.apache.beam.sdk.options.Validation.Required

import common.Config._

/**
  * Loader specific CLI configuration
  * Unlike Mutator, required --key=value format and ignores unknown options (for Dataflow)
  */
object CommandLine {
  import implicits._

  trait Options extends PipelineOptions with StreamingOptions {
    @Required
    @Description("Base64-encoded self-describing JSON configuration of " +
      "com.snowplowanalytics.snowplow.storage/bigquery_config/jsonschema/1-0-0 schema")
    def getConfig: ValueProvider[String]
    def setConfig(value: ValueProvider[String]): Unit

    @Required
    @Description("Base64-encoded self-describing JSON configuration of Iglu resolver")
    def getResolver: ValueProvider[String]
    def setResolver(value: ValueProvider[String]): Unit
  }

  final def getEnvironment(options: Options): ValueProvider[Environment] = {
    val config = options.getConfig.map(c => decodeBase64Json(c).fold(throw _, identity))
    val resolver = options.getResolver.map(r => decodeBase64Json(r).fold(throw _, identity))

    config.ap(resolver) { (c, r) =>
      transform(EnvironmentConfig(r, c)).fold(throw _, identity)
    }
  }
}

