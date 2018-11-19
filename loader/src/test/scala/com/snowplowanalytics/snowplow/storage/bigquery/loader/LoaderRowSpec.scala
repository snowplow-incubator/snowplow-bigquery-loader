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

import cats.data.NonEmptyList
import cats.syntax.either._

import org.joda.time.Instant

import com.google.api.services.bigquery.model.TableRow

import org.json4s.jackson.JsonMethods.parse
import org.json4s.JsonAST.JObject

import com.snowplowanalytics.iglu.client.Resolver

class LoaderRowSpec extends org.specs2.Specification { def is = s2"""
  parseContexts groups contexts with same version $e1
  fromJson works with only collector_tstamp $e2
  fromJson fails without collector_tstamp $e3
  fromJson transforms all JSON types into its AnyRef counterparts $e4
  fromJson tries to fetch a schema for context $e5
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

    val result = LoaderRow
      .parseSelfDescribingJson(SpecHelpers.resolver)(contextsProperty)
      .toEither
      .map(x => x.map { case (k, v) => (k, v.asInstanceOf[java.util.List[String]].size())} toMap)

    result must beRight(Map(
      "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0" -> 2,
      "contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0" -> 1
    ))
  }

  def e2 = {
    val input = parse("""{"collector_tstamp": "2018-09-01T17:48:34.000Z"}""").asInstanceOf[JObject]

    val result = LoaderRow.fromJson(SpecHelpers.resolver)(input)
    val tstamp = Instant.parse("2018-09-01T17:48:34.000Z")
    val tableRow = new TableRow().set("collector_tstamp", "2018-09-01T17:48:34.000Z")

    val expected = (tableRow, tstamp)
    result.map(_._1) must beRight(expected._1)
  }

  def e3 = {
    val input = parse("""{}""").asInstanceOf[JObject]

    val result = LoaderRow.fromJson(SpecHelpers.resolver)(input)

    val expected = NonEmptyList.of("No collector_tstamp")
    result must beLeft(expected)
  }

  def e4 = {
    val input = parse("""{
      "collector_tstamp": "2018-09-01T17:48:34.000Z",
      "txn_id": null,
      "geo_location": "12.0,1.0",
      "domain_sessionidx": 3,
      "br_cookies": false
    }""").asInstanceOf[JObject]

    val resolver = Resolver(0, Nil, None)
    val result = LoaderRow.fromJson(resolver)(input)
    val tstamp = Instant.parse("2018-09-01T17:48:34.000Z")
    val tableRow = new TableRow()
      .set("collector_tstamp", "2018-09-01T17:48:34.000Z")
      .set("domain_sessionidx", 3)
      .set("br_cookies", false)

    val expected = (tableRow, tstamp)
    result must beRight(expected)
  }

  def e5 = {
    val input = parse("""{
      "collector_tstamp": "2018-09-01T17:48:34.000Z",
      "contexts": {
        "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
        "data": [
          {
            "schema": "iglu:com.acme/context/jsonschema/1-0-0",
            "data": {}
          }
        ]
      }
    }""").asInstanceOf[JObject]

    val resolver = Resolver(0, Nil, None)
    val result = LoaderRow.fromJson(resolver)(input).leftMap(x => x.map(_.take(76)))

    val expected = NonEmptyList.of("error: Could not find schema with key iglu:com.acme/context/jsonschema/1-0-0")
    result must beLeft(expected)
  }
}
