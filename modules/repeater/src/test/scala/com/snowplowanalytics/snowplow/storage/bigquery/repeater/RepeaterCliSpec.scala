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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.{GcsPath, ListenCommand, validateBucket}

import cats.data.NonEmptyList
import org.specs2.mutable.Specification

class RepeaterCliSpec extends Specification {
  "parse" should {
    "extract valid Repeater configuration" in {
      val expected = ListenCommand(SpecHelpers.configs.repeaterEnv, 20, 30, 900, false)
      val result =
        RepeaterCli.parse(
          Seq(
            "--config",
            SpecHelpers.configs.validHoconB64,
            "--resolver",
            SpecHelpers.configs.validResolverJsonB64,
            "--bufferSize",
            "20",
            "--timeout",
            "30",
            "--backoffPeriod",
            "900"
          )
        )

      result must beRight(expected)
    }

    "fail to extract valid configuration" in {
      val invalidHoconRes = RepeaterCli.parse(
        Seq(
          "--config",
          SpecHelpers.configs.invalidHoconB64,
          "--resolver",
          SpecHelpers.configs.validResolverJsonB64,
          "--bufferSize",
          "20",
          "--timeout",
          "30",
          "--backoffPeriod",
          "900"
        )
      )
      val malformedJsonRes = RepeaterCli.parse(
        Seq(
          "--config",
          SpecHelpers.configs.validHoconB64,
          "--resolver",
          SpecHelpers.configs.malformedResolverJsonB64,
          "--bufferSize",
          "20",
          "--timeout",
          "30",
          "--backoffPeriod",
          "900"
        )
      )
      val invalidJsonRes = RepeaterCli.parse(
        Seq(
          "--config",
          SpecHelpers.configs.validHoconB64,
          "--resolver",
          SpecHelpers.configs.invalidResolverJsonB64,
          "--bufferSize",
          "20",
          "--timeout",
          "30",
          "--backoffPeriod",
          "900"
        )
      )

      List(invalidHoconRes, malformedJsonRes, invalidJsonRes).forall(_ must beLeft)
    }
  }

  "validateBucket" should {
    "succeed with a valid input" in {
      val input    = "gs://my-bucket/my-folder"
      val expected = GcsPath("my-bucket", "my-folder/")

      validateBucket(input).toEither must beRight(expected)
    }

    "fail if the 'gs://' prefix is missing" in {
      val input    = "my-bucket/my-folder"
      val expected = NonEmptyList.one("GCS bucket must start with gs://")

      validateBucket(input).toEither must beLeft(expected)
    }

    "fail if only the 'gs://' protocol is specified" in {
      val input    = "gs://"
      val expected = NonEmptyList.one("GCS bucket cannot be empty")

      validateBucket(input).toEither must beLeft(expected)
    }
  }
}
