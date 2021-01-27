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

import org.joda.time.Instant
import com.google.api.services.bigquery.model.TableRow
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.bigquery.Row.{Primitive, Record, Repeated}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Adapter._
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
    "transform all JSON types into their AnyRef counterparts" >> {
      // INPUTS
      val contexts = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"id": "deadbeef-0000-1111-2222-deadbeef3333"}"""
          )
        )
      )
      val derivedContexts = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.snowplowanalytics.snowplow", "derived_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"a": "b"}"""
          )
        )
      )
      val unstructEvent = UnstructEvent(
        Some(
          SelfDescribingData(
            SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"c": "d"}"""
          )
        )
      )

      val inputPlain = SpecHelpers
        .ExampleEvent
        .copy(
          br_cookies        = Some(false),
          domain_sessionidx = Some(3)
        )
      val inputC = SpecHelpers
        .ExampleEvent
        .copy(
          contexts          = contexts,
          br_cookies        = Some(false),
          domain_sessionidx = Some(3)
        )
      val inputDc = SpecHelpers
        .ExampleEvent
        .copy(
          br_cookies        = Some(false),
          domain_sessionidx = Some(3),
          derived_contexts  = derivedContexts
        )
      val inputUe = SpecHelpers
        .ExampleEvent
        .copy(
          br_cookies        = Some(false),
          domain_sessionidx = Some(3),
          unstruct_event    = unstructEvent
        )
      val inputUeC = SpecHelpers
        .ExampleEvent
        .copy(
          contexts          = contexts,
          br_cookies        = Some(false),
          domain_sessionidx = Some(3),
          unstruct_event    = unstructEvent
        )
      val inputUeDc = SpecHelpers
        .ExampleEvent
        .copy(
          br_cookies        = Some(false),
          domain_sessionidx = Some(3),
          unstruct_event    = unstructEvent,
          derived_contexts  = derivedContexts
        )
      val inputUeDcC = SpecHelpers
        .ExampleEvent
        .copy(
          contexts          = contexts,
          br_cookies        = Some(false),
          domain_sessionidx = Some(3),
          unstruct_event    = unstructEvent,
          derived_contexts  = derivedContexts
        )

      // OUTPUTS
      val resultPlain = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputPlain)
      val resultC     = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputC)
      val resultDc    = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputDc)
      val resultUe    = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputUe)
      val resultUeC   = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputUeC)
      val resultUeDc  = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputUeDc)
      val resultUeDcC = LoaderRow.fromEvent(SpecHelpers.resolver, processor)(inputUeDcC)

      // EXPECTATIONS
      val cValue  = adaptRow(Repeated(List(Record(List(("id", Primitive("deadbeef-0000-1111-2222-deadbeef3333")))))))
      val dcValue = adaptRow(Repeated(List(Record(List(("a", Primitive("b")))))))
      val ueValue = adaptRow(Record(List(("c", Primitive("d")))))

      val tableRowPlain = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
      val tableRowC = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_web_page_1_0_0", cValue)
      val tableRowDc = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_derived_context_1_0_0", dcValue)
      val tableRowUe = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
      val tableRowUeC = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_web_page_1_0_0", cValue)
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
      val tableRowUeDc = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
        .set("contexts_com_snowplowanalytics_snowplow_derived_context_1_0_0", dcValue)
      val tableRowUeDcC = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_web_page_1_0_0", cValue)
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
        .set("contexts_com_snowplowanalytics_snowplow_derived_context_1_0_0", dcValue)

      // format: off
      val expectedPlain = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowPlain, inputPlain.inventory)
      val expectedC     = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowC, inputC.inventory)
      val expectedDc    = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowDc, inputDc.inventory)
      val expectedUe    = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowUe, inputUe.inventory)
      val expectedUeC   = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowUeC, inputUeC.inventory)
      val expectedUeDc  = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowUeDc, inputUeDc.inventory)
      val expectedUeDcC = LoaderRow(new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli), tableRowUeDcC, inputUeDcC.inventory)
      // format: on

      // OUTCOMES
      resultPlain must beRight(expectedPlain)
      resultC must beRight(expectedC)
      resultDc must beRight(expectedDc)
      resultUe must beRight(expectedUe)
      resultUeC must beRight(expectedUeC)
      resultUeDc must beRight(expectedUeDc)
      resultUeDcC must beRight(expectedUeDcC)
    }
  }
}
