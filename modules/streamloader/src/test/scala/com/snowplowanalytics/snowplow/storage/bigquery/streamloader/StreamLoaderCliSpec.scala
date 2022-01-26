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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers

import org.specs2.mutable.Specification

// The inputs for these tests take into account all settings specified in common/src/main/resources/application.conf
class StreamLoaderCliSpec extends Specification {
  "parse" should {
    "extract valid Loader configuration" in {
      val expected = SpecHelpers.configs.loaderEnv
      val result =
        StreamLoaderCli.parse(
          Seq("--config", SpecHelpers.configs.validHoconB64, "--resolver", SpecHelpers.configs.validResolverJsonB64)
        )

      result must beRight(expected)
    }

    "fail to extract Loader configuration from invalid inputs" in {
      val invalidHoconRes = StreamLoaderCli.parse(
        Seq("--config", SpecHelpers.configs.invalidHoconB64, "--resolver", SpecHelpers.configs.validResolverJsonB64)
      )
      val malformedJsonRes = StreamLoaderCli.parse(
        Seq("--config", SpecHelpers.configs.validHoconB64, "--resolver", SpecHelpers.configs.malformedResolverJsonB64)
      )
      val invalidJsonRes = StreamLoaderCli.parse(
        Seq("--config", SpecHelpers.configs.validHoconB64, "--resolver", SpecHelpers.configs.invalidResolverJsonB64)
      )

      List(invalidHoconRes, malformedJsonRes, invalidJsonRes).forall(_ must beLeft)
    }
  }
}
