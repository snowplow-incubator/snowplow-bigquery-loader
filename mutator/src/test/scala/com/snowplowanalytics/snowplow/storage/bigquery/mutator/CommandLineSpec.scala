/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import java.util.UUID

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.EnvironmentConfig
import com.snowplowanalytics.snowplow.storage.bigquery.mutator.CommandLine._

class CommandLineSpec extends org.specs2.Specification {
  def is = s2"""
  parse extracts valid configuration for listen subcommand $e1
  parse extracts valid configuration for create subcommand $e2
  getEnv validates end extracts configuration $e3
  """

  def e1 = {
    val expected = ListenCommand(EnvironmentConfig(SpecHelpers.jsonResolver, SpecHelpers.jsonConfig), false)
    val result =
      CommandLine.parse(Seq("listen", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
    result must beRight(expected)
  }

  def e2 = {
    val expected = CreateCommand(EnvironmentConfig(SpecHelpers.jsonResolver, SpecHelpers.jsonConfig))
    val result =
      CommandLine.parse(Seq("create", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
    result must beRight(expected)
  }

  def e3 = {
    val expected = Config(
      "Snowplow BigQuery",
      UUID.fromString("ff5176f8-c0e3-4ef0-a94f-3b4f86e042ca"),
      "enriched-topic",
      "snowplow-data",
      "atomic",
      "events",
      Config.LoadMode.StreamingInserts(false),
      "types-topic",
      "types-sub",
      "bad-rows-topic",
      "failed-inserts-topic"
    )

    val result =
      CommandLine.parse(Seq("create", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
//      .map(_.getEnv.unsafeRunSync().config)
    result must beRight(expected)

    skipped("Config schema is not on Iglu Central yet")
  }
}
