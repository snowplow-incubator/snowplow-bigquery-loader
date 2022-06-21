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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.{CliConfig, EncodedHoconOrPath, EncodedJsonOrPath}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.EncodedOrPath.FromBase64

import com.monovore.decline.Argument
import com.typesafe.config.ConfigFactory

import org.specs2.mutable.Specification

class CliConfigSpec extends Specification {
  "EncodedJsonOrPath parser " should {
    "succeed with valid JSON" in {
      val input    = SpecHelpers.configs.validResolverJsonB64
      val result   = implicitly[Argument[EncodedJsonOrPath]].read(input).toEither
      val expected = SpecHelpers.configs.validResolverJson
      result must beRight(FromBase64(expected))
    }

    "fail with invalid JSON" in {
      val input  = SpecHelpers.configs.malformedResolverJsonB64
      val result = implicitly[Argument[EncodedJsonOrPath]].read(input).toEither
      result must beLeft
    }
  }

  "EncodedHoconOrPath" should {
    "succeed with valid HOCON" in {
      val input  = SpecHelpers.configs.validHoconB64
      val result = implicitly[Argument[EncodedHoconOrPath]].read(input).toEither
      result must beRight
    }

    "fail with invalid HOCON" in {
      val input  = SpecHelpers.configs.invalidHoconB64
      val result = implicitly[Argument[EncodedHoconOrPath]].read(input).toEither
      result must beLeft
    }
  }

  "AllAppConfigs fromRaw" should {
    "succeed with valid json and hocon" in {
      val hocon    = ConfigFactory.parseString(SpecHelpers.configs.validHocon)
      val resolver = SpecHelpers.configs.validResolverJson
      val input    = CliConfig.Raw(Some(FromBase64(hocon)), FromBase64(resolver))
      val result   = CliConfig.parseRaw(input)
      result must beRight
    }
    "fail with valid JSON which does not validate against iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1" in {
      val hocon    = ConfigFactory.parseString(SpecHelpers.configs.validHocon)
      val resolver = SpecHelpers.configs.invalidResolverJson
      val input    = CliConfig.Raw(Some(FromBase64(hocon)), FromBase64(resolver))
      val result   = CliConfig.parseRaw(input)
      result must beLeft
    }
  }
}
