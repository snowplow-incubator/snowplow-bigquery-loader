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

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Row.{Primitive, Record, Repeated}
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.storage.bigquery.common.Adapter._
import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow.{transformJson, LoadTstampField}
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers.implicits.idClock
import cats.Id
import com.google.api.services.bigquery.model.TableRow
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import io.circe.literal._
import io.circe.parser.parse
import org.joda.time.Instant
import org.specs2.mutable.Specification

class LoaderRowSpec extends Specification {
  val processor: Processor   = SpecHelpers.meta.processor
  val resolver: Resolver[Id] = SpecHelpers.iglu.resolver
  val fieldCache: FieldCache[Id] = SpecHelpers.cache.fieldCache

  "groupContexts" should {
    "group contexts with same version" in {
      val contexts = SpecHelpers.events.geoContexts

      val result = LoaderRow
        .groupContexts(resolver, fieldCache, contexts)
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
    "transform all JSON types into their AnyRef counterparts" in {
      // INPUTS
      val event           = SpecHelpers.events.exampleEvent
      val contexts        = SpecHelpers.events.contexts
      val derivedContexts = SpecHelpers.events.derivedContexts
      val unstructEvent   = SpecHelpers.events.unstructEvent

      val inputPlain = event.copy(
        br_cookies        = Some(false),
        domain_sessionidx = Some(3)
      )
      val inputC = event.copy(
        contexts          = contexts,
        br_cookies        = Some(false),
        domain_sessionidx = Some(3)
      )
      val inputDc = event.copy(
        br_cookies        = Some(false),
        domain_sessionidx = Some(3),
        derived_contexts  = derivedContexts
      )
      val inputUe = event.copy(
        br_cookies        = Some(false),
        domain_sessionidx = Some(3),
        unstruct_event    = unstructEvent
      )
      val inputUeC = event.copy(
        contexts          = contexts,
        br_cookies        = Some(false),
        domain_sessionidx = Some(3),
        unstruct_event    = unstructEvent
      )
      val inputUeDc = event.copy(
        br_cookies        = Some(false),
        domain_sessionidx = Some(3),
        unstruct_event    = unstructEvent,
        derived_contexts  = derivedContexts
      )
      val inputUeDcC = event.copy(
        contexts          = contexts,
        br_cookies        = Some(false),
        domain_sessionidx = Some(3),
        unstruct_event    = unstructEvent,
        derived_contexts  = derivedContexts
      )

      // OUTPUTS
      val resultPlain = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputPlain)
      val resultC     = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputC)
      val resultDc    = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputDc)
      val resultUe    = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputUe)
      val resultUeC   = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputUeC)
      val resultUeDc  = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputUeDc)
      val resultUeDcC = LoaderRow.fromEvent(resolver, processor, fieldCache)(inputUeDcC)

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
        .set(LoadTstampField.name, "AUTO")
      val tableRowC = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_web_page_1_0_0", cValue)
        .set(LoadTstampField.name, "AUTO")
      val tableRowDc = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_derived_context_1_0_0", dcValue)
        .set(LoadTstampField.name, "AUTO")
      val tableRowUe = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
        .set(LoadTstampField.name, "AUTO")
      val tableRowUeC = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("contexts_com_snowplowanalytics_snowplow_web_page_1_0_0", cValue)
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
        .set(LoadTstampField.name, "AUTO")
      val tableRowUeDc = new TableRow()
        .set("v_collector", "bq-loader-test")
        .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
        .set("br_cookies", false)
        .set("domain_sessionidx", 3)
        .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
        .set("v_etl", "bq-loader-test")
        .set("unstruct_event_com_snowplowanalytics_snowplow_unstruct_event_1_0_0", ueValue)
        .set("contexts_com_snowplowanalytics_snowplow_derived_context_1_0_0", dcValue)
        .set(LoadTstampField.name, "AUTO")
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
        .set(LoadTstampField.name, "AUTO")

      // format: off
      val tstamp = event.collector_tstamp.toEpochMilli

      val expectedPlain = LoaderRow(new Instant(tstamp), tableRowPlain, inputPlain.inventory)
      val expectedC     = LoaderRow(new Instant(tstamp), tableRowC, inputC.inventory)
      val expectedDc    = LoaderRow(new Instant(tstamp), tableRowDc, inputDc.inventory)
      val expectedUe    = LoaderRow(new Instant(tstamp), tableRowUe, inputUe.inventory)
      val expectedUeC   = LoaderRow(new Instant(tstamp), tableRowUeC, inputUeC.inventory)
      val expectedUeDc  = LoaderRow(new Instant(tstamp), tableRowUeDc, inputUeDc.inventory)
      val expectedUeDcC = LoaderRow(new Instant(tstamp), tableRowUeDcC, inputUeDcC.inventory)
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

  "transformJson" should {
    "succeed with an event whose schema has an [array, null] property" in {
      val json = parse(SpecHelpers.events.nullableArrayUnstructData).getOrElse(json"""{"n": "a"}""")
      val result =
        transformJson(
          resolver,
          fieldCache,
          SchemaKey("com.snowplowanalytics.snowplow", "nullable_array_event", "jsonschema", Full(1, 0, 0))
        )(json).toEither

      result must beRight
    }
  }

  "parse" should {
    "succeed with an event whose schema has an [array, null] property" in {
      LoaderRow.parse(resolver, processor, fieldCache)(SpecHelpers.events.nullableArrayUnstructEvent) must beRight
    }
  }
}
