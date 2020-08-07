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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import org.joda.time.Instant
import com.google.api.services.bigquery.model.TableRow

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers._

import org.specs2.mutable.Specification

class LoaderRowSpec extends Specification {
  "groupContexts" should {
    "group contexts with same version" >> {
      val contexts = Vector(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
        ),
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"latitude": 0, "longitude": 1.1}"""
        ),
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 1, 0)),
          json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": null}"""
        )
      )

      val result = LoaderRow
        .groupContexts(SpecHelpers.resolver, contexts)
        .toEither
        .map(x => x.map { case (k, v) => (k, v.asInstanceOf[java.util.List[String]].size()) }.toMap)

      result must beRight(
        Map(
          "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0" -> 2,
          "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0" -> 1
        )
      )
    }
  }

  "fromEvent" should {
    "transform all JSON types into its AnyRef counterparts" >> {
      val input  = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
      val result = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(input)
      val tableRow = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")

      val expected =
        LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRow, input.inventory)
      result must beRight(expected)
    }
  }
}
