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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers
import com.snowplowanalytics.snowplow.storage.bigquery.mutator.MutatorCli._

import com.google.cloud.bigquery.TimePartitioning

import org.specs2.mutable.Specification

class MutatorCliSpec extends Specification {
  "parse" should {
    val validHoconB64        = SpecHelpers.configs.validHoconB64
    val validResolverJsonB64 = SpecHelpers.configs.validResolverJsonB64

    "extract valid configuration" in {
      val mutatorEnv = SpecHelpers.configs.mutatorEnv

      "for listen subcommand" in {
        val expected = MutatorCommand.Listen(mutatorEnv, false)
        val result =
          MutatorCli.parse(Seq("listen", "--config", validHoconB64, "--resolver", validResolverJsonB64))

        result must beRight(expected)
      }

      "for create subcommand with partitioning" in {
        val expected = MutatorCommand.Create(
          mutatorEnv,
          Some(Atomic.table.find(_.name == "derived_tstamp").get),
          TimePartitioning.Type.DAY,
          true
        )
        val result =
          MutatorCli.parse(
            Seq(
              "create",
              "--config",
              validHoconB64,
              "--resolver",
              validResolverJsonB64,
              "--partitionColumn",
              "derived_tstamp",
              "--partitioningType",
              "day",
              "--requirePartitionFilter"
            )
          )

        result must beRight(expected)
      }

      "for create subcommand without partitioning" in {
        val expected = MutatorCommand.Create(mutatorEnv, None, TimePartitioning.Type.DAY, false)
        val result =
          MutatorCli.parse(Seq("create", "--config", validHoconB64, "--resolver", validResolverJsonB64))

        result must beRight(expected)
      }

      "for add-column subcommand" in {
        val expected = MutatorCommand.AddColumn(mutatorEnv, SpecHelpers.iglu.adClickSchemaKey, Data.UnstructEvent)
        val result = MutatorCli.parse(
          Seq(
            "add-column",
            "--config",
            validHoconB64,
            "--resolver",
            validResolverJsonB64,
            "--schema",
            "iglu:com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0",
            "--shred-property",
            "UNSTRUCT_EVENT"
          )
        )

        result must beRight(expected)
      }
    }

    "fail to extract valid configuration" in {
      "with invalid HOCON" in {
        val invalidHoconB64 = SpecHelpers.configs.invalidHoconB64
        val listenRes       = MutatorCli.parse(Seq("listen", "--config", invalidHoconB64, "--resolver", validResolverJsonB64))
        val createRes       = MutatorCli.parse(Seq("create", "--config", invalidHoconB64, "--resolver", validResolverJsonB64))
        val addColumnRes = MutatorCli.parse(
          Seq(
            "add-column",
            "--config",
            invalidHoconB64,
            "--resolver",
            validResolverJsonB64,
            "--schema",
            "iglu:com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0",
            "--shred-property",
            "UNSTRUCT_EVENT"
          )
        )

        List(listenRes, createRes, addColumnRes).forall(_ must beLeft)
      }

      "with malformed JSON" in {
        val malformedResolverJsonB64 = SpecHelpers.configs.malformedResolverJsonB64
        val listenRes =
          MutatorCli.parse(Seq("listen", "--config", validHoconB64, "--resolver", malformedResolverJsonB64))
        val createRes =
          MutatorCli.parse(Seq("create", "--config", validHoconB64, "--resolver", malformedResolverJsonB64))
        val addColumnRes = MutatorCli.parse(
          Seq(
            "add-column",
            "--config",
            validHoconB64,
            "--resolver",
            malformedResolverJsonB64,
            "--schema",
            "iglu:com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0",
            "--shred-property",
            "UNSTRUCT_EVENT"
          )
        )

        List(listenRes, createRes, addColumnRes).forall(_ must beLeft)
      }

      "with invalid self-describing JSON" in {
        val invalidResolverJsonB64 = SpecHelpers.configs.invalidResolverJsonB64
        val listenRes              = MutatorCli.parse(Seq("listen", "--config", validHoconB64, "--resolver", invalidResolverJsonB64))
        val createRes              = MutatorCli.parse(Seq("create", "--config", validHoconB64, "--resolver", invalidResolverJsonB64))
        val addColumnRes = MutatorCli.parse(
          Seq(
            "add-column",
            "--config",
            validHoconB64,
            "--resolver",
            invalidResolverJsonB64,
            "--schema",
            "iglu:com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0",
            "--shred-property",
            "UNSTRUCT_EVENT"
          )
        )

        List(listenRes, createRes, addColumnRes).forall(_ must beLeft)
      }
    }
  }
}
