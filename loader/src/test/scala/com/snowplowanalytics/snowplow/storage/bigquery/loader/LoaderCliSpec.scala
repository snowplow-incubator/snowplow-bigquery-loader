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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.spotify.scio.ContextAndArgs
import org.specs2.mutable.Specification

class LoaderCliSpec extends Specification {
  "parse" should {
    val cliArgs   = Array(s"--config=${SpecHelpers.base64Config}", s"--resolver=${SpecHelpers.base64Resolver}")
    val (_, args) = ContextAndArgs(cliArgs)

    "extract valid Loader configuration" in {
      val expected = SpecHelpers.loaderEnv
      val result =
        LoaderCli.parse(args)

      result must beRight(expected)
    }
  }
}
