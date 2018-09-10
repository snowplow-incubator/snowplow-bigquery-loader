package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import org.json4s.jackson.JsonMethods.parse
import LoaderRow.parseSelfDescribingJson
import org.json4s.JsonAST.JObject

class LoaderRowSpec extends org.specs2.Specification { def is = s2"""
  parseContexts is valid $e1
  """

  def e1 = {
    val contextsProperty = parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
        |  "data": [
        |    {
        |      "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0",
        |      "data": {
        |         "latitude": 22,
        |         "longitude": 23.1,
        |		      "latitudeLongitudeAccuracy": 23
        |      }
        |    },
        |    {
        |      "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0",
        |      "data": {
        |         "latitude": 0,
        |         "longitude": 1.1
        |      }
        |    },
        |    {
        |      "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0",
        |      "data": {
        |         "latitude": 22,
        |         "longitude": 23.1,
        |		      "latitudeLongitudeAccuracy": null
        |      }
        |    }
        |  ]
        |}
      """.stripMargin).asInstanceOf[JObject]
    val result = LoaderRow.parseSelfDescribingJson(SpecHelpers.resolver)(contextsProperty)

    ok
  }
}
