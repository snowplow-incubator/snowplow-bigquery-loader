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
package forwarder

import cats.syntax.either._
import com.spotify.scio.Args
import org.apache.beam.sdk.options.{Description, PipelineOptions, StreamingOptions, ValueProvider}
import common.Config._
import org.apache.beam.sdk.options.Validation.Required

import implicits._

/**
  * Forwarder specific CLI configuration
  * Unlike Mutator, required --key=value format and ignores unknown options (for Dataflow)
  * Unlike Loader, also required `--failedInsertsSub`
  */
object CommandLine {
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

    @Required
    @Description("Subscription attached to failedInserts topic")
    def getFailedInsertsSub: ValueProvider[String]
    def setFailedInsertsSub(value: ValueProvider[String]): Unit
  }

  case class ForwarderEnvironment(common: Environment, failedInserts: String) {
    def getFullFailedInsertsSub: String = s"projects/${common.config.projectId}/subscriptions/$failedInserts"
  }

  final def getEnvironment(options: Options): ValueProvider[ForwarderEnvironment] = {
    val config = options.getConfig.map(c => decodeBase64Json(c).fold(throw _, identity))
    val resolver = options.getResolver.map(r => decodeBase64Json(r).fold(throw _, identity))
    val failedInserts = options.getFailedInsertsSub

    config.ap(resolver) { (c, r) =>
      transform(EnvironmentConfig(r, c)).fold(throw _, identity)
    }.ap(failedInserts) { (e, i) =>
      ForwarderEnvironment(e, i)
    }
  }
}
