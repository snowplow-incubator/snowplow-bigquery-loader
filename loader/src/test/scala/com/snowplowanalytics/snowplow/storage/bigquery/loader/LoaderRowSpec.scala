/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import scala.collection.JavaConverters._
import io.circe.literal._
import org.joda.time.Instant
import com.google.api.services.bigquery.model.TableRow
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class LoaderRowSpec extends org.specs2.Specification { def is = s2"""
  fromEvent transforms all JSON types into its AnyRef counterparts $e1
  fromEvent transforms event with context into its AnyRef counterparts $e2
  fromEvent transforms event with unstruct event into its AnyRef counterparts $e3
  fromEvent transforms event with unstruct event, contexts and derived_contexts into its AnyRef counterparts $e4
  """

  def e1 = {
    val input = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
    val result = LoaderRow.fromEvent(SpecHelpers.resolver)(input)
    val tableRow = new TableRow()
      .set("v_collector", "bq-loader-test")
      .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
      .set("br_cookies", false)
      .set("domain_sessionidx", 3)
      .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
      .set("v_etl", "bq-loader-test")

    val expected = (tableRow, new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli))
    result must beRight(expected)
  }

  def e2 = {
    val contexts = List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      )
    )
    val input = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3), contexts = Contexts(contexts))
    val result = LoaderRow.fromEvent(SpecHelpers.resolver)(input)
    val tableRow = new TableRow()
      .set("v_collector", "bq-loader-test")
      .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
      .set("br_cookies", false)
      .set("domain_sessionidx", 3)
      .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
      .set("v_etl", "bq-loader-test")
      .set(
        "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0",
        List(
          new TableRow()
          .set("latitude", BigDecimal(22).bigDecimal)
          .set("longitude", BigDecimal(23.1).bigDecimal)
          .set("altitude", null)
          .set("altitude_accuracy", null)
          .set("bearing", null)
          .set("latitude_longitude_accuracy", BigDecimal(23).bigDecimal)
          .set("speed", null)
        ).asJava
      )

    val expected = (tableRow, new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli))
    result must beRight(expected)
  }

  def e3 = {
    val selfDescribingData = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
    )
    val input = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3), unstruct_event = UnstructEvent(Some(selfDescribingData)))
    val result = LoaderRow.fromEvent(SpecHelpers.resolver)(input)
    val tableRow = new TableRow()
      .set("v_collector", "bq-loader-test")
      .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
      .set("br_cookies", false)
      .set("domain_sessionidx", 3)
      .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
      .set("v_etl", "bq-loader-test")
      .set(
        "unstruct_event_com_snowplowanalytics_snowplow_geolocation_context_1_0_0",
        new TableRow()
          .set("latitude", BigDecimal(22).bigDecimal)
          .set("longitude", BigDecimal(23.1).bigDecimal)
          .set("altitude", null)
          .set("altitude_accuracy", null)
          .set("bearing", null)
          .set("latitude_longitude_accuracy", BigDecimal(23).bigDecimal)
          .set("speed", null)
      )

    val expected = (tableRow, new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli))
    result must beRight(expected)
  }

  def e4 = {
    val contexts = Contexts(
      List(
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
    )

    val derived_contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 1, 0)),
          json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": null}"""
        )
      )
    )

    val unstructEvent = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
        )
      )
    )

    val input = SpecHelpers.ExampleEvent.copy(
      br_cookies = Some(false),
      domain_sessionidx = Some(3),
      contexts = contexts,
      derived_contexts = derived_contexts,
      unstruct_event = unstructEvent
    )
    val result = LoaderRow.fromEvent(SpecHelpers.resolver)(input)
    val tableRow = new TableRow()
      .set("v_collector", "bq-loader-test")
      .set("collector_tstamp", "2019-02-18T08:06:07.580Z")
      .set("br_cookies", false)
      .set("domain_sessionidx", 3)
      .set("event_id", "ba553b7f-63d5-47ad-8697-06016b472c34")
      .set("v_etl", "bq-loader-test")
      .set(
        "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0",
        List(
          new TableRow()
            .set("latitude", BigDecimal(22).bigDecimal)
            .set("longitude", BigDecimal(23.1).bigDecimal)
            .set("altitude", null)
            .set("altitude_accuracy", null)
            .set("bearing", null)
            .set("latitude_longitude_accuracy", BigDecimal(23).bigDecimal)
            .set("speed", null),
          new TableRow()
            .set("latitude", BigDecimal(0).bigDecimal)
            .set("longitude", BigDecimal(1.1).bigDecimal)
            .set("altitude", null)
            .set("altitude_accuracy", null)
            .set("bearing", null)
            .set("latitude_longitude_accuracy", null)
            .set("speed", null)
        ).asJava
      )
      .set(
        "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0",
        List(
          new TableRow()
            .set("latitude", BigDecimal(22).bigDecimal)
            .set("longitude", BigDecimal(23.1).bigDecimal)
            .set("altitude", null)
            .set("altitude_accuracy", null)
            .set("bearing", null)
            .set("latitude_longitude_accuracy", null)
            .set("speed", null),
          new TableRow()
            .set("latitude", BigDecimal(22).bigDecimal)
            .set("longitude", BigDecimal(23.1).bigDecimal)
            .set("altitude", null)
            .set("altitude_accuracy", null)
            .set("bearing", null)
            .set("latitude_longitude_accuracy", null)
            .set("speed", null)
        ).asJava
      )
      .set(
        "unstruct_event_com_snowplowanalytics_snowplow_geolocation_context_1_0_0",
        new TableRow()
          .set("latitude", BigDecimal(22).bigDecimal)
          .set("longitude", BigDecimal(23.1).bigDecimal)
          .set("altitude", null)
          .set("altitude_accuracy", null)
          .set("bearing", null)
          .set("latitude_longitude_accuracy", BigDecimal(23).bigDecimal)
          .set("speed", null)
      )

    val expected = (tableRow, new Instant(SpecHelpers.ExampleEvent.collector_tstamp.toEpochMilli))
    result must beRight(expected)
  }
}
