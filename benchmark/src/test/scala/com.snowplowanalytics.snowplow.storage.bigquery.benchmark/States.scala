package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Utils
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.parse
import org.openjdk.jmh.annotations.{Scope, State}

object States {

  @State(Scope.Benchmark)
  class ExampleEventState {
    val resolver = Resolver(0, Nil, None)
    val baseEvent = parse(
      """{
      "collector_tstamp": "2018-09-01T17:48:34.000Z",
      "txn_id": null,
      "geo_location": "12.0,1.0",
      "domain_sessionidx": 3,
      "br_cookies": false
    }""").asInstanceOf[JObject]
    val adClickSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1, 0, 0))
    val adClickJson = """{
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
                            |""".stripMargin
    val unstruct = Utils.toCirce(parse(
      s"""{
      "collector_tstamp": "2018-09-01T17:48:34.000Z",
      "txn_id": null,
      "geo_location": "12.0,1.0",
      "domain_sessionidx": 3,
      "br_cookies": false,
      "unstruct_event": $adClickJson
    }""").asInstanceOf[JObject])
    val contexts = Vector(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        Utils.toCirce(parse(s"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """).asInstanceOf[JObject])
      ),
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        Utils.toCirce(parse(s"""{"latitude": 0, "longitude": 1.1}""").asInstanceOf[JObject])
      ),
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 1, 0)),
        Utils.toCirce(parse(s"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": null}""").asInstanceOf[JObject])
      )
    )
  }
}
