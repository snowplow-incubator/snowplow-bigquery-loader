package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.fasterxml.jackson.databind.JsonNode
import com.snowplowanalytics.iglu.client.repositories.{RepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey, Validated}
import org.json4s.jackson.JsonMethods.asJsonNode
import org.json4s.jackson.parseJson
import scalaz.Success

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

  val resolver = Resolver(10, InMemoryRef(schemas))
}
