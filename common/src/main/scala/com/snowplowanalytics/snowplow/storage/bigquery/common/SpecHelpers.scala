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

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS, TimeUnit}
import cats.Id
import cats.effect.Clock
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingData, SelfDescribingSchema}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.badrows.Processor
import io.circe.literal._

object SpecHelpers {
  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)

    override def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
  }

  val processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  private[bigquery] val adClick = json"""{
    	"$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    	"description": "Schema for an ad click event",
    	"type": "object",
    	"properties": {
    		"clickId": {"type": "string"},
    		"impressionId": {"type": "string"},
    		"zoneId": {"type": "string"},
    		"bannerId": {"type": "string"},
    		"campaignId": {"type": "string"},
    		"advertiserId": {"type": "string"},
    		"targetUrl": {"type": "string","minLength":1},
    		"costModel": {"enum": ["cpa", "cpc", "cpm"]},
    		"cost": {	"type": "number",	"minimum": 0}
    	},
    	"required": ["targetUrl"],
    	"dependencies": {"cost": ["costModel"]},
    	"additionalProperties": false
    }
    """
  private val geolocation100    = json"""{
     "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
     "description": "Schema for client geolocation contexts",
     "type": "object",
     "properties": {
        "latitude": {"type": "number","minimum": -90,"maximum": 90},
        "longitude": {"type": "number","minimum": -180,"maximum": 180},
     		"latitudeLongitudeAccuracy": {"type": "number"},
     		"altitude": {"type": "number"},
     		"altitudeAccuracy": {"type": "number"},
     		"bearing": {"type": "number"},
     		"speed": {"type": "number"}
     },
     "required": ["latitude", "longitude"],
     "additionalProperties": false
     }
    """
  private val geolocation110    = json"""
      {
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "description": "Schema for client geolocation contexts",
      "type": "object",
      "properties": {
         "latitude": {"type": "number","minimum": -90,"maximum": 90},
         "longitude": {"type": "number","minimum": -180,"maximum": 180},
      		"latitudeLongitudeAccuracy": {"type": ["number", "null"]},
      		"altitude": {"type": ["number", "null"]},
      		"altitudeAccuracy": {"type": ["number", "null"]},
      		"bearing": {"type": ["number", "null"]},
      		"speed": {"type": ["number", "null"]}
      },
      "required": ["latitude", "longitude"],
      "additionalProperties": false
      }
    """
  private val webPage           = json"""
      {
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "description": "Schema for a web page context",
      "type": "object",
      "properties": {
          "id": {
              "type": "string",
              "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$$"
          }
      },
      "required": ["id"],
      "additionalProperties": false
      }
    """
  private val derivedContext =
    json"""
      {
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "description": "Schema for a test derived context",
      "type": "object",
      "properties": {
          "a": {
              "type": "string"
          }
      },
      "required": ["a"],
      "additionalProperties": false
      }
    """
  private val unstructEvent = {
    json"""
      {
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "description": "Schema for a test unstruct event",
      "type": "object",
      "properties": {
          "c": {
              "type": "string"
          }
      },
      "required": ["c"],
      "additionalProperties": false
      }
    """
  }
  private val nullableArrayEvent =
    json"""
      {
        "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "description": "Schema for an unstruct event with a field whose type is a nullable array",
        "type": "object",
        "properties": {
          "entities": {
            "type": [
              "array",
              "null"
            ],
            "items": {
              "type": [
                "string",
                "null"
              ],
              "enum": [
                null,
                "not-null"
              ]
            }
          }
        }
      }"""

  private[bigquery] val contexts = Vector(
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

  val schemas = Map(
    SchemaMap("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1, 0, 0))             -> adClick,
    SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0))  -> geolocation100,
    SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 1, 0))  -> geolocation110,
    SchemaMap("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0))             -> webPage,
    SchemaMap("com.snowplowanalytics.snowplow", "derived_context", "jsonschema", SchemaVer.Full(1, 0, 0))      -> derivedContext,
    SchemaMap("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, 0, 0))       -> unstructEvent,
    SchemaMap("com.snowplowanalytics.snowplow", "nullable_array_event", "jsonschema", SchemaVer.Full(1, 0, 0)) -> nullableArrayEvent
  )

  // format: off
  def emptyEvent(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Event =
    Event(None, None, None, collectorTstamp, None, None, id, None, None, None, vCollector, vTstamp, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, UnstructEvent(None), None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, None, None, None)
  // format: on

  val ExampleEvent = emptyEvent(
    UUID.fromString("ba553b7f-63d5-47ad-8697-06016b472c34"),
    Instant.ofEpochMilli(1550477167580L),
    "bq-loader-test",
    "bq-loader-test"
  )

  val nullableArrayUnstructData = """{"entities": null}"""
  val nullableArrayUnstructJson =
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
       "schema": "iglu:com.snowplowanalytics.snowplow/nullable_array_event/jsonschema/1-0-0",
       "data": $nullableArrayUnstructData
      }
    }"""
  val nullableArrayUnstruct = UnstructEvent(
    Some(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "nullable_array_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
        nullableArrayUnstructJson
      )
    )
  )
  val nullableArrayUnstructEvent = ExampleEvent.copy(unstruct_event = nullableArrayUnstruct).toTsv

  object IdInstances {
    implicit val idClock: Clock[Id] = new Clock[Id] {
      def realTime(unit: TimeUnit): Id[Long] =
        unit.convert(System.currentTimeMillis(), MILLISECONDS)

      def monotonic(unit: TimeUnit): Id[Long] =
        unit.convert(System.nanoTime(), NANOSECONDS)
    }
  }

  val StaticRegistry: Registry = Registry.InMemory(Registry.Config("InMemory", 1, List.empty), schemas.toList.map {
    case (k, v) => SelfDescribingSchema(k, v)
  })

  val resolver = Resolver[Id](List(StaticRegistry), None)

  val validResolverJson =
    json"""{"schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":500,"repositories":[{"name":"Iglu Central","priority":0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri":"http://iglucentral.com"}}}]}}"""

  val validResolverJsonB64 =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="

  // Dangling open {
  val invalidJsonB64 =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfQ=="

  // Missing required cacheSize property
  val invalidResolverJsonB64 =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJyZXBvc2l0b3JpZXMiOlt7Im5hbWUiOiJJZ2x1IENlbnRyYWwiLCJwcmlvcml0eSI6MCwidmVuZG9yUHJlZml4ZXMiOlsiY29tLnNub3dwbG93YW5hbHl0aWNzIl0sImNvbm5lY3Rpb24iOnsiaHR0cCI6eyJ1cmkiOiJodHRwOi8vaWdsdWNlbnRyYWwuY29tIn19fV19fQ=="

  val validHoconB64 =
    "ewogICJwcm9qZWN0SWQiOiAic25vd3Bsb3ctZGF0YSIKCiAgImxvYWRlciI6IHsKICAgICJpbnB1dCI6IHsKICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAic3Vic2NyaXB0aW9uIjogImVucmljaGVkLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6IHsKICAgICAgICAidHlwZSI6ICJCaWdRdWVyeSIKICAgICAgICAiZGF0YXNldElkIjogImF0b21pYyIKICAgICAgICAidGFibGVJZCI6ICJldmVudHMiCiAgICAgIH0KCiAgICAgICJiYWQiOiB7CiAgICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAgICJ0b3BpYyI6ICJiYWQtdG9waWMiCiAgICAgIH0KCiAgICAgICJ0eXBlcyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogInR5cGVzLXRvcGljIgogICAgICB9CgogICAgICAiZmFpbGVkSW5zZXJ0cyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogImZhaWxlZC1pbnNlcnRzLXRvcGljIgogICAgICB9CiAgICB9CgogICAgImxvYWRNb2RlIjogewogICAgICAidHlwZSI6ICJTdHJlYW1pbmdJbnNlcnRzIgogICAgICAicmV0cnkiOiBmYWxzZQogICAgfQogIH0KCiAgIm11dGF0b3IiOiB7CiAgICAiaW5wdXQiOiB7CiAgICAgICJ0eXBlIjogIlB1YlN1YiIKICAgICAgInN1YnNjcmlwdGlvbiI6ICJtdXRhdG9yLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6ICR7bG9hZGVyLm91dHB1dC5nb29kfQogICAgfQogIH0KCiAgInJlcGVhdGVyIjogewogICAgImlucHV0IjogewogICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICJzdWJzY3JpcHRpb24iOiAiZmFpbGVkLWluc2VydHMtc3ViIgogICAgfQoKICAgICJvdXRwdXQiOiB7CiAgICAgICJnb29kIjogJHtsb2FkZXIub3V0cHV0Lmdvb2R9CgogICAgICAiZGVhZExldHRlcnMiOiB7CiAgICAgICAgInR5cGUiOiAiR2NzIgogICAgICAgICJidWNrZXQiOiAiZ3M6Ly9zb21lLWJ1Y2tldC8iCiAgICAgIH0KICAgIH0KICB9CgogICJ0YWdzIjogewogICAgImlkIjogImZmNTE3NmY4LWMwZTMtNGVmMC1hOTRmLTNiNGY4NmUwNDJjYSIKICAgICJuYW1lIjogIlNub3dwbG93IEJpZ1F1ZXJ5IgogIH0KfQ=="

  // Dangling open {
  val invalidHoconB64 =
    "ewogICJwcm9qZWN0SWQiOiAic25vd3Bsb3ctZGF0YSIKCiAgImxvYWRlciI6IHsKICAgICJpbnB1dCI6IHsKICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAic3Vic2NyaXB0aW9uIjogImVucmljaGVkLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6IHsKICAgICAgICAidHlwZSI6ICJCaWdRdWVyeSIKICAgICAgICAiZGF0YXNldElkIjogImF0b21pYyIKICAgICAgICAidGFibGVJZCI6ICJldmVudHMiCiAgICAgIH0KCiAgICAgICJiYWQiOiB7CiAgICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAgICJ0b3BpYyI6ICJiYWQtdG9waWMiCiAgICAgIH0KCiAgICAgICJ0eXBlcyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogInR5cGVzLXRvcGljIgogICAgICB9CgogICAgICAiZmFpbGVkSW5zZXJ0cyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogImZhaWxlZC1pbnNlcnRzLXRvcGljIgogICAgICB9CiAgICB9CgogICAgImxvYWRNb2RlIjogewogICAgICAidHlwZSI6ICJTdHJlYW1pbmdJbnNlcnRzIgogICAgICAicmV0cnkiOiBmYWxzZQogICAgfQogIH0KCiAgIm11dGF0b3IiOiB7CiAgICAiaW5wdXQiOiB7CiAgICAgICJ0eXBlIjogIlB1YlN1YiIKICAgICAgInN1YnNjcmlwdGlvbiI6ICJtdXRhdG9yLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6ICR7bG9hZGVyLm91dHB1dC5nb29kfQogICAgfQogIH0KCiAgInJlcGVhdGVyIjogewogICAgImlucHV0IjogewogICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICJzdWJzY3JpcHRpb24iOiAiZmFpbGVkLWluc2VydHMtc3ViIgogICAgfQoKICAgICJvdXRwdXQiOiB7CiAgICAgICJnb29kIjogJHtsb2FkZXIub3V0cHV0Lmdvb2R9CgogICAgICAiZGVhZExldHRlcnMiOiB7CiAgICAgICAgInR5cGUiOiAiR2NzIgogICAgICAgICJidWNrZXQiOiAiZ3M6Ly9zb21lLWJ1Y2tldC8iCiAgICAgIH0KICAgIH0KICB9CgogICJ0YWdzIjogewogICAgImlkIjogImZmNTE3NmY4LWMwZTMtNGVmMC1hOTRmLTNiNGY4NmUwNDJjYSIKICAgICJuYW1lIjogIlNub3dwbG93IEJpZ1F1ZXJ5IgogIH0K"
}
