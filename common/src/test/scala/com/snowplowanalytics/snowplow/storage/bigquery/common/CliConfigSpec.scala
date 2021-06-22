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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.{decodeBase64Hocon, decodeBase64Json}
import org.specs2.mutable.Specification

class CliConfigSpec extends Specification {
  "decodeBase64Json" should {
    "succeed with valid JSON" in {
      decodeBase64Json(SpecHelpers.validResolverJsonB64) must beRight(SpecHelpers.validResolverJson)
    }

    "fail with invalid JSON" in {
      decodeBase64Json(SpecHelpers.invalidJsonB64) must beLeft
    }

    "fail with valid JSON which does not validate against iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1" in {
      decodeBase64Json(SpecHelpers.invalidResolverJsonB64) must beLeft
    }
  }

  "decodeBase64Hocon" should {
    "succeed with valid HOCON" in {
      decodeBase64Hocon(SpecHelpers.validHoconB64) must beRight
    }
    "fail with invalid HOCON" in {
      decodeBase64Hocon(SpecHelpers.invalidHoconB64) must beLeft
    }
  }
}
