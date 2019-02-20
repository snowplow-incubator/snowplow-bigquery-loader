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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import java.util.UUID
import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.Id
import cats.effect.Clock

import io.circe.literal._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

object SpecHelpers {

  private val adClick = json"""{
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
  private val geolocation100 = json"""{
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
  private val geolocation110 = json"""
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

  val schemas = Map(
    SchemaMap("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1,0,0)) -> adClick,
    SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1,0,0)) -> geolocation100,
    SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1,1,0)) -> geolocation110
  )

  def emptyEvent(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Event =
    Event(None, None, None, collectorTstamp, None, None, id, None, None, None, vCollector, vTstamp, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, UnstructEvent(None), None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, None, None, None)

  val ExampleEvent = emptyEvent(
    UUID.fromString("ba553b7f-63d5-47ad-8697-06016b472c34"),
    Instant.ofEpochMilli(1550477167580L),
    "bq-loader-test",
    "bq-loader-test")

  object IdInstances {
    implicit val idClock: Clock[Id] = new Clock[Id] {
      def realTime(unit: TimeUnit): Id[Long] =
        unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

      def monotonic(unit: TimeUnit): Id[Long] =
        unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
    }
  }

  val StaticRegistry: Registry = Registry.InMemory(
    Registry.Config("InMemory", 1, List.empty),
    schemas.toList.map { case (k, v) => SelfDescribingSchema(k, v)})

  val resolver = Resolver[Id](List(StaticRegistry), None)
}
