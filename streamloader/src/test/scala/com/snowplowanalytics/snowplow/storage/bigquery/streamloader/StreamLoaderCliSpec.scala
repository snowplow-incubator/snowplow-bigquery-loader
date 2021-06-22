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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import org.specs2.mutable.Specification

class StreamLoaderCliSpec extends Specification {
  "parse" should {
    "extract valid Loader configuration" in {
      val expected = SpecHelpers.loaderEnv
      val result =
        StreamLoaderCli.parse(Seq("--config", SpecHelpers.base64Config, "--resolver", SpecHelpers.base64Resolver))

      result must beRight(expected)
    }
  }
}
