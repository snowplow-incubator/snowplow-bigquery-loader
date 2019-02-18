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

import scalaz.Success

import org.json4s.jackson.JsonMethods.{ asJsonNode, parse => parseJson }

import com.fasterxml.jackson.databind.JsonNode

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{ Contexts, UnstructEvent }

import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey, Validated}
import com.snowplowanalytics.iglu.client.repositories.{RepositoryRef, RepositoryRefConfig}

object SpecHelpers {

  case class InMemoryRef(schemas: Map[SchemaKey, JsonNode]) extends RepositoryRef {

    val classPriority: Int = 1
    val descriptor: String = "In memory registry"
    val config: RepositoryRefConfig = RepositoryRefConfig(descriptor, 1, List("com.snowplowanalytics"))

    def lookupSchema(schemaKey: SchemaKey): Validated[Option[JsonNode]] =
      Success(schemas.get(schemaKey))
  }

  private val adClick = asJsonNode(parseJson("""{
    |	"$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    |	"description": "Schema for an ad click event",
    |	"self": {	"vendor":"com.snowplowanalytics.snowplow","name":"ad_click","format":"jsonschema","version":"1-0-0"},
    |	"type": "object",
    |	"properties": {
    |		"clickId": {"type": "string"},
    |		"impressionId": {"type": "string"},
    |		"zoneId": {"type": "string"},
    |		"bannerId": {"type": "string"},
    |		"campaignId": {"type": "string"},
    |		"advertiserId": {"type": "string"},
    |		"targetUrl": {"type": "string","minLength":1},
    |		"costModel": {"enum": ["cpa", "cpc", "cpm"]},
    |		"cost": {	"type": "number",	"minimum": 0}
    |	},
    |	"required": ["targetUrl"],
    |	"dependencies": {"cost": ["costModel"]},
    |	"additionalProperties": false
    |}
    |""".stripMargin))
  private val geolocation100 = asJsonNode(parseJson(
    """
      |{
      |"$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      |"description": "Schema for client geolocation contexts",
      |"self": {"vendor": "com.snowplowanalytics.snowplow","name": "geolocation_context","format": "jsonschema","version": "1-0-0"},
      |"type": "object",
      |"properties": {
      |   "latitude": {"type": "number","minimum": -90,"maximum": 90},
      |   "longitude": {"type": "number","minimum": -180,"maximum": 180},
      |		"latitudeLongitudeAccuracy": {"type": "number"},
      |		"altitude": {"type": "number"},
      |		"altitudeAccuracy": {"type": "number"},
      |		"bearing": {"type": "number"},
      |		"speed": {"type": "number"}
      |},
      |"required": ["latitude", "longitude"],
      |"additionalProperties": false
      |}
    """.stripMargin))
  private val geolocation110 = asJsonNode(parseJson(
    """
      |{
      |"$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      |"description": "Schema for client geolocation contexts",
      |"self": {"vendor": "com.snowplowanalytics.snowplow","name":"geolocation_context","format":"jsonschema","version":"1-1-0"},
      |"type": "object",
      |"properties": {
      |   "latitude": {"type": "number","minimum": -90,"maximum": 90},
      |   "longitude": {"type": "number","minimum": -180,"maximum": 180},
      |		"latitudeLongitudeAccuracy": {"type": ["number", "null"]},
      |		"altitude": {"type": ["number", "null"]},
      |		"altitudeAccuracy": {"type": ["number", "null"]},
      |		"bearing": {"type": ["number", "null"]},
      |		"speed": {"type": ["number", "null"]}
      |},
      |"required": ["latitude", "longitude"],
      |"additionalProperties": false
      |}
    """.stripMargin))

  val schemas = Map(
    SchemaKey("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", "1-0-0") -> adClick,
    SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-0-0") -> geolocation100,
    SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0") -> geolocation110
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

  val resolver = Resolver(10, InMemoryRef(schemas))
}
