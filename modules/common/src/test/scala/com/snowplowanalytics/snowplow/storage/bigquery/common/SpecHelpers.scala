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

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.{
  LoaderEnvironment,
  MutatorEnvironment,
  RepeaterEnvironment
}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model._
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Config.{
  LoaderOutputs,
  MutatorOutput,
  RepeaterOutputs
}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.LoadMode.StreamingInserts
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring.{
  Dropwizard,
  Statsd,
  Stdout,
  Sentry => SentryConfig
}

import cats.{Applicative, Id}
import cats.effect.Clock
import io.circe.Json
import io.circe.literal._
import io.circe.parser.parse

import java.time.Instant
import java.util.UUID
import java.net.URI
import scala.concurrent.duration.{FiniteDuration, HOURS, MILLISECONDS, NANOSECONDS, SECONDS}
import scala.io.Source

object SpecHelpers {
  object meta {
    val processor: Processor = Processor(generated.ProjectSettings.name, generated.ProjectSettings.version)
  }

  object utils {
    def createContexts(flattenSchemaKey: String): Json =
      json"""{
                $flattenSchemaKey:[
                  {
                    "name":"sp",
                    "value":"05d09faa707a9ff092e49929c79630d1"
                  },
                  {
                    "name":"sp",
                    "value":"b73833bf7e3af5a6d5a02e79be96b064"
                  }
                ]
              }"""

    def createUnstructEvent(flattenSchemaKey: String): Json =
      json"""{
                $flattenSchemaKey: {
                  "chrome_first_paint":1458561380397,
                  "connect_end":1458561378602,
                  "connect_start":1458561378435
                }
              }"""

    def parseSchema(string: String): Schema =
      Schema
        .parse(parse(string).fold(throw _, identity))
        .getOrElse(throw new RuntimeException("SpecHelpers.parseSchema received invalid JSON Schema"))
  }

  object implicits {
    implicit val idClock: Clock[Id] = new Clock[Id] {
      def realTime: FiniteDuration =
        FiniteDuration(System.currentTimeMillis, MILLISECONDS)

      def monotonic: FiniteDuration =
        FiniteDuration(System.nanoTime(), NANOSECONDS)

      def applicative: Applicative[Id] = implicitly[Applicative[Id]]
    }
  }

  object iglu {
    private[bigquery] val adClick =
      json"""{
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
              }"""

    private val geolocation100 =
      json"""{
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
              }"""

    private val geolocation110 =
      json"""{
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
              }"""

    private val webPage =
      json"""{
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
              }"""

    private val derivedContext =
      json"""{
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
              }"""

    private val unstructEvent =
      json"""{
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
              }"""

    private val nullableArrayEvent =
      json"""{
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

    private val schemas = Map(
      SchemaMap("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1, 0, 0))             -> adClick,
      SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0))  -> geolocation100,
      SchemaMap("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 1, 0))  -> geolocation110,
      SchemaMap("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0))             -> webPage,
      SchemaMap("com.snowplowanalytics.snowplow", "derived_context", "jsonschema", SchemaVer.Full(1, 0, 0))      -> derivedContext,
      SchemaMap("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, 0, 0))       -> unstructEvent,
      SchemaMap("com.snowplowanalytics.snowplow", "nullable_array_event", "jsonschema", SchemaVer.Full(1, 0, 0)) -> nullableArrayEvent
    )

    private val staticRegistry = Registry.InMemory(Registry.Config("InMemory", 1, List.empty), schemas.toList.map {
      case (k, v) => SelfDescribingSchema(k, v)
    })

    private[bigquery] val resolver: Resolver[Id] = Resolver[Id](List(staticRegistry), None)

    private[bigquery] val adClickSchemaKey =
      SchemaKey("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1, 0, 0))
  }

  object events {
    private[bigquery] val geoContexts = Vector(
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

    private[bigquery] val contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"id": "deadbeef-0000-1111-2222-deadbeef3333"}"""
        )
      )
    )

    private[bigquery] val derivedContexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "derived_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"a": "b"}"""
        )
      )
    )

    private[bigquery] val unstructEvent = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"c": "d"}"""
        )
      )
    )

    private[bigquery] val adClickUnstructEvent = UnstructEvent(
      Some(
        SelfDescribingData(
          iglu.adClickSchemaKey,
          json"""{"targetUrl": "example.com"}"""
        )
      )
    )

    // format: off
    private def emptyEvent(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Event =
      Event(None, None, None, collectorTstamp, None, None, id, None, None, None, vCollector, vTstamp, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        Contexts(Nil), None, None, None, None, None, UnstructEvent(None), None, None, None, None, None, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        Contexts(Nil), None, None, None, None, None, None, None, None)
    // format: on

    private[bigquery] val exampleEvent = emptyEvent(
      UUID.fromString("ba553b7f-63d5-47ad-8697-06016b472c34"),
      Instant.ofEpochMilli(1550477167580L),
      "bq-loader-test",
      "bq-loader-test"
    )

    private[bigquery] val nullableArrayUnstructData = """{"entities": null}"""

    private val nullableArrayUnstructJson =
      json"""{
                "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
                "data": {
                  "schema": "iglu:com.snowplowanalytics.snowplow/nullable_array_event/jsonschema/1-0-0",
                  "data": $nullableArrayUnstructData
                }
              }"""

    private val nullableArrayUnstruct = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "nullable_array_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          nullableArrayUnstructJson
        )
      )
    )

    private[bigquery] val nullableArrayUnstructEvent = exampleEvent.copy(unstruct_event = nullableArrayUnstruct).toTsv

    private[bigquery] val reconstructableEvent =
      json"""{
                "page_urlhost":"snowplowanalytics.com",
                "br_features_realplayer":false,
                "etl_tstamp":"2018-12-18T15:07:17.970Z",
                "collector_tstamp" : "2016-03-21T11:56:32.844Z",
                "dvce_ismobile":false,
                "br_version":"49.0.2623.87",
                "v_collector":"ssc-0.6.0-kinesis",
                "os_family":"Mac OS X",
                "event_vendor":"com.snowplowanalytics.snowplow",
                "network_userid":"05d09faa707a9ff092e49929c79630d1",
                "br_renderengine":"WEBKIT",
                "br_lang":"en-US",
                "user_fingerprint":"262ac8a29684b99251deb81d2e246446",
                "page_urlscheme":"http",
                "pp_yoffset_min":1786,
                "br_features_quicktime":false,
                "event":"page_ping",
                "user_ipaddress":"086271e520deb84db3259c0af2878ee5",
                "br_features_pdf":true,
                "doc_height":3017,
                "br_features_flash":true,
                "os_manufacturer":"Apple Inc.",
                "br_colordepth":"24",
                "event_format":"jsonschema",
                "pp_xoffset_min":0,
                "doc_width":1591,
                "br_family":"Chrome",
                "useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36 BMID/E6797F732B",
                "event_name":"page_ping",
                "os_name":"Mac OS X",
                "page_urlpath":"/",
                "br_name":"Chrome 49",
                "page_title":"Snowplow – Your digital nervous system",
                "dvce_created_tstamp":"2016-03-21T11:56:31.811Z",
                "br_features_gears":false,
                "dvce_type":"Computer",
                "dvce_sent_tstamp":"2016-03-21T11:56:31.813Z",
                "br_features_director":false,
                "user_id":"50d9ee525d12b25618e2dbab1f25c39d",
                "v_tracker":"js-2.6.0",
                "os_timezone":"Europe/London",
                "br_type":"Browser",
                "br_features_windowsmedia":false,
                "event_version":"1-0-0",
                "dvce_screenwidth":1680,
                "domain_sessionid":"bfcfa2c3-9b9e-4023-a850-9873e52e0fd2",
                "domain_userid":"9506c862a9dbeea2f55fba6af4a0e7ec",
                "name_tracker":"snplow6",
                "dvce_screenheight":1050,
                "br_features_java":false,
                "br_viewwidth":1591,
                "br_viewheight":478,
                "br_features_silverlight":false,
                "br_cookies":true,
                "derived_tstamp":"2016-03-21T11:56:32.842Z",
                "app_id":"snowplowweb",
                "pp_yoffset_max":2123,
                "domain_sessionidx":10,
                "pp_xoffset_max":0,
                "page_urlport":80,
                "platform":"web",
                "event_id":"4fbd682f-3395-46dd-8aa0-ed0c1f5f1d92",
                "page_url":"http://snowplowanalytics.com/",
                "doc_charset":"UTF-8",
                "event_fingerprint":"1be0be1f32b2854847f96bb83479ea44",
                "v_etl":"spark-1.16.0-common-0.35.0",
                "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0":[
                  {
                    "id":"3e4a5deb-1504-4cb2-8751-9d358c632328"
                  }
                ],
                "contexts_org_w3_performance_timing_1_0_0":[
                  {
                    "chrome_first_paint":1458561380397,
                    "connect_end":1458561378602,
                    "connect_start":1458561378435,
                    "dom_complete":1458561383683,
                    "dom_content_loaded_event_end":1458561380395,
                    "dom_content_loaded_event_start":1458561380392,
                    "dom_interactive":1458561380392,
                    "dom_loading":1458561379095,
                    "domain_lookup_end":1458561378435,
                    "domain_lookup_start":1458561378211,
                    "fetch_start":1458561378195,
                    "load_event_end":1458561383737,
                    "load_event_start":1458561383683,
                    "ms_first_paint":null,
                    "navigation_start":1458561378194,
                    "proxy_end":null,
                    "proxy_start":null,
                    "redirect_end":0,
                    "redirect_start":0,
                    "request_end":null,
                    "request_start":1458561378602,
                    "response_end":1458561379080,
                    "response_start":1458561379079,
                    "secure_connection_start":0,
                    "unload_event_end":1458561379080,
                    "unload_event_start":1458561379080
                  }
                ],
                "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0":[
                  {
                    "device_family":"Other",
                    "os_family":"Mac OS X",
                    "useragent_family":"Chrome",
                    "os_major":"10",
                    "os_minor":"10",
                    "os_patch":"5",
                    "os_patch_minor":null,
                    "os_version":"Mac OS X 10.10.5",
                    "useragent_major":"49",
                    "useragent_minor":"0",
                    "useragent_patch":"2623",
                    "useragent_version":"Chrome 49.0.2623"
                  }
                ],
                "contexts_org_ietf_http_cookie_1_0_0":[
                  {
                    "name":"sp",
                    "value":"05d09faa707a9ff092e49929c79630d1"
                  },
                  {
                    "name":"sp",
                    "value":"b73833bf7e3af5a6d5a02e79be96b064"
                  }
                ]
              }"""

    private[bigquery] val requiredFieldMissingEventJson =
      json"""{
                "collector_tstamp" : "2016-03-21T11:56:32.844Z",
                "event_id" : "ba553b7f-63d5-47ad-8697-06016b472c34",
                "v_collector": "bq-loader-test"
              }"""
  }

  object configs {
    private[bigquery] val validResolverJson =
      json"""{
                "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1",
                "data": {
                  "cacheSize": 500,
                  "repositories": [
                   {
                      "name": "Iglu Central",
                      "priority": 0,
                      "vendorPrefixes": [ "com.snowplowanalytics" ],
                      "connection": {
                        "http": {
                          "uri":"http://iglucentral.com"
                        }
                      }
                    }
                  ]
                }
              }"""

    // Missing required `cacheSize` property
    private[bigquery] val invalidResolverJson =
      json"""{
                "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1",
                "data": {
                  "repositories": [
                   {
                      "name": "Iglu Central",
                      "priority": 0,
                      "vendorPrefixes": [ "com.snowplowanalytics" ],
                      "connection": {
                        "http": {
                          "uri":"http://iglucentral.com"
                        }
                      }
                    }
                  ]
                }
              }"""

    private[bigquery] val validResolverJsonB64 =
      "ewogICJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuaWdsdS9yZXNvbHZlci1jb25maWcvanNvbnNjaGVtYS8xLTAtMSIsCiAgImRhdGEiOiB7CiAgICAiY2FjaGVTaXplIjogNTAwLAogICAgInJlcG9zaXRvcmllcyI6IFsKICAgICB7CiAgICAgICAgIm5hbWUiOiAiSWdsdSBDZW50cmFsIiwKICAgICAgICAicHJpb3JpdHkiOiAwLAogICAgICAgICJ2ZW5kb3JQcmVmaXhlcyI6IFsgImNvbS5zbm93cGxvd2FuYWx5dGljcyIgXSwKICAgICAgICAiY29ubmVjdGlvbiI6IHsKICAgICAgICAgICJodHRwIjogewogICAgICAgICAgICAidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSIKICAgICAgICAgIH0KICAgICAgICB9CiAgICAgIH0KICAgIF0KICB9Cn0="

    // Dangling open '{'
    private[bigquery] val malformedResolverJsonB64 =
      "ewogICJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuaWdsdS9yZXNvbHZlci1jb25maWcvanNvbnNjaGVtYS8xLTAtMSIsCiAgImRhdGEiOiB7CiAgICAiY2FjaGVTaXplIjogNTAwLAogICAgInJlcG9zaXRvcmllcyI6IFsKICAgICB7CiAgICAgICAgIm5hbWUiOiAiSWdsdSBDZW50cmFsIiwKICAgICAgICAicHJpb3JpdHkiOiAwLAogICAgICAgICJ2ZW5kb3JQcmVmaXhlcyI6IFsgImNvbS5zbm93cGxvd2FuYWx5dGljcyIgXSwKICAgICAgICAiY29ubmVjdGlvbiI6IHsKICAgICAgICAgICJodHRwIjogewogICAgICAgICAgICAidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSIKICAgICAgICAgIH0KICAgICAgICB9CiAgICAgIH0KICAgIF0KICB9"

    // Missing required `cacheSize` property
    private[bigquery] val invalidResolverJsonB64 =
      "ewogICJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuaWdsdS9yZXNvbHZlci1jb25maWcvanNvbnNjaGVtYS8xLTAtMSIsCiAgImRhdGEiOiB7CiAgICAicmVwb3NpdG9yaWVzIjogWwogICAgIHsKICAgICAgICAibmFtZSI6ICJJZ2x1IENlbnRyYWwiLAogICAgICAgICJwcmlvcml0eSI6IDAsCiAgICAgICAgInZlbmRvclByZWZpeGVzIjogWyAiY29tLnNub3dwbG93YW5hbHl0aWNzIiBdLAogICAgICAgICJjb25uZWN0aW9uIjogewogICAgICAgICAgImh0dHAiOiB7CiAgICAgICAgICAgICJ1cmkiOiJodHRwOi8vaWdsdWNlbnRyYWwuY29tIgogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgXQogIH0KfQ=="

    private[bigquery] val validHoconB64 =
      "ewogICJwcm9qZWN0SWQiOiAic25vd3Bsb3ctZGF0YSIKCiAgImxvYWRlciI6IHsKICAgICJpbnB1dCI6IHsKICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAic3Vic2NyaXB0aW9uIjogImVucmljaGVkLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6IHsKICAgICAgICAidHlwZSI6ICJCaWdRdWVyeSIKICAgICAgICAiZGF0YXNldElkIjogImF0b21pYyIKICAgICAgICAidGFibGVJZCI6ICJldmVudHMiCiAgICAgIH0KCiAgICAgICJiYWQiOiB7CiAgICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAgICJ0b3BpYyI6ICJiYWQtdG9waWMiCiAgICAgIH0KCiAgICAgICJ0eXBlcyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogInR5cGVzLXRvcGljIgogICAgICB9CgogICAgICAiZmFpbGVkSW5zZXJ0cyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogImZhaWxlZC1pbnNlcnRzLXRvcGljIgogICAgICB9CiAgICB9CgogICAgImxvYWRNb2RlIjogewogICAgICAidHlwZSI6ICJTdHJlYW1pbmdJbnNlcnRzIgogICAgICAicmV0cnkiOiBmYWxzZQogICAgfQogIH0KCiAgIm11dGF0b3IiOiB7CiAgICAiaW5wdXQiOiB7CiAgICAgICJ0eXBlIjogIlB1YlN1YiIKICAgICAgInN1YnNjcmlwdGlvbiI6ICJtdXRhdG9yLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6ICR7bG9hZGVyLm91dHB1dC5nb29kfQogICAgfQogIH0KCiAgInJlcGVhdGVyIjogewogICAgImlucHV0IjogewogICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICJzdWJzY3JpcHRpb24iOiAiZmFpbGVkLWluc2VydHMtc3ViIgogICAgfQoKICAgICJvdXRwdXQiOiB7CiAgICAgICJnb29kIjogJHtsb2FkZXIub3V0cHV0Lmdvb2R9CgogICAgICAiZGVhZExldHRlcnMiOiB7CiAgICAgICAgInR5cGUiOiAiR2NzIgogICAgICAgICJidWNrZXQiOiAiZ3M6Ly9zb21lLWJ1Y2tldC8iCiAgICAgIH0KICAgIH0KICB9CgogICJtb25pdG9yaW5nIjogewogICAgInN0YXRzZCI6IHsKICAgICAgImhvc3RuYW1lIjogInN0YXRzZC5hY21lLmdsIgogICAgICAicG9ydCI6IDEwMjQKICAgICAgInRhZ3MiOiB7fQogICAgICAicGVyaW9kIjogIjEgc2VjIgogICAgICAicHJlZml4IjogInNub3dwbG93Lm1vbml0b3JpbmciCiAgICB9LAogICAgInN0ZG91dCI6IHsKICAgICAgInBlcmlvZCI6ICIxIHNlYyIKICAgICAgInByZWZpeCI6ICJzbm93cGxvdy5tb25pdG9yaW5nIgogICAgfSwKICAgICJzZW50cnkiOiB7CiAgICAgICJkc24iOiAiaHR0cDovL3NlbnRyeS5hY21lLmNvbSIKICAgIH0sCiAgICAiZHJvcHdpemFyZCI6IHsKICAgICAgInBlcmlvZCI6ICIxIHNlYyIKICAgIH0KICB9Cn0="

    private[bigquery] val validHocon =
      Source.fromResource("valid-config.hocon").mkString

    // Dangling open '{'
    private[bigquery] val invalidHoconB64 =
      "ewogICJwcm9qZWN0SWQiOiAic25vd3Bsb3ctZGF0YSIKCiAgImxvYWRlciI6IHsKICAgICJpbnB1dCI6IHsKICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAic3Vic2NyaXB0aW9uIjogImVucmljaGVkLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6IHsKICAgICAgICAidHlwZSI6ICJCaWdRdWVyeSIKICAgICAgICAiZGF0YXNldElkIjogImF0b21pYyIKICAgICAgICAidGFibGVJZCI6ICJldmVudHMiCiAgICAgIH0KCiAgICAgICJiYWQiOiB7CiAgICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAgICJ0b3BpYyI6ICJiYWQtdG9waWMiCiAgICAgIH0KCiAgICAgICJ0eXBlcyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogInR5cGVzLXRvcGljIgogICAgICB9CgogICAgICAiZmFpbGVkSW5zZXJ0cyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogImZhaWxlZC1pbnNlcnRzLXRvcGljIgogICAgICB9CiAgICB9CgogICAgImxvYWRNb2RlIjogewogICAgICAidHlwZSI6ICJTdHJlYW1pbmdJbnNlcnRzIgogICAgICAicmV0cnkiOiBmYWxzZQogICAgfQogIH0KCiAgIm11dGF0b3IiOiB7CiAgICAiaW5wdXQiOiB7CiAgICAgICJ0eXBlIjogIlB1YlN1YiIKICAgICAgInN1YnNjcmlwdGlvbiI6ICJtdXRhdG9yLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6ICR7bG9hZGVyLm91dHB1dC5nb29kfQogICAgfQogIH0KCiAgInJlcGVhdGVyIjogewogICAgImlucHV0IjogewogICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICJzdWJzY3JpcHRpb24iOiAiZmFpbGVkLWluc2VydHMtc3ViIgogICAgfQoKICAgICJvdXRwdXQiOiB7CiAgICAgICJnb29kIjogJHtsb2FkZXIub3V0cHV0Lmdvb2R9CgogICAgICAiZGVhZExldHRlcnMiOiB7CiAgICAgICAgInR5cGUiOiAiR2NzIgogICAgICAgICJidWNrZXQiOiAiZ3M6Ly9zb21lLWJ1Y2tldC8iCiAgICAgIH0KICAgIH0KICB9CgogICJtb25pdG9yaW5nIjogewogICAgInN0YXRzZCI6IHsKICAgICAgImhvc3RuYW1lIjogInN0YXRzZC5hY21lLmdsIgogICAgICAicG9ydCI6IDEwMjQKICAgICAgInRhZ3MiOiB7fQogICAgICAicGVyaW9kIjogIjEgc2VjIgogICAgICAicHJlZml4IjogInNub3dwbG93Lm1vbml0b3JpbmciCiAgICB9LAogICAgImRyb3B3aXphcmQiOiB7CiAgICAgICJwZXJpb2QiOiAiMSBzZWMiCiAgICB9CiAgfQ=="

    private val lSubscription: String        = "enriched-sub"
    private val lInput: Input.PubSub         = Input.PubSub(lSubscription)
    private val datasetId: String            = "atomic"
    private val tableId: String              = "events"
    private val good: Output.BigQuery        = Output.BigQuery(datasetId, tableId)
    private val badTopic: String             = "bad-topic"
    private val bad: Output.PubSub           = Output.PubSub(badTopic)
    private val typesTopic: String           = "types-topic"
    private val types: Output.PubSub         = Output.PubSub(typesTopic)
    private val failedInsertsTopic: String   = "failed-inserts-topic"
    private val failedInserts: Output.PubSub = Output.PubSub(failedInsertsTopic)
    private val lOutput: LoaderOutputs       = LoaderOutputs(good, bad, types, failedInserts)
    private val loadMode: LoadMode           = StreamingInserts(false)

    private val maxQueueSize: Int      = 3000
    private val maxRequestBytes: Int   = 50000000
    private val parallelPullCount: Int = 3
    private val maxAckExtensionPeriod  = FiniteDuration(1L, HOURS)
    private val awaitTerminatePeriod   = FiniteDuration(30L, SECONDS)
    private val consumerSettings =
      ConsumerSettings(maxQueueSize, maxRequestBytes, parallelPullCount, maxAckExtensionPeriod, awaitTerminatePeriod)

    private val bqWriteRequestThreshold: Int          = 500
    private val bqWriteRequestTimeout: FiniteDuration = FiniteDuration(1L, SECONDS)
    private val bqWriteRequestSizeLimit: Int          = 10000000
    private val goodSinkConcurrency: Int              = 1024
    private val sinkSettingsGood = SinkSettings.Good(
      bqWriteRequestThreshold,
      bqWriteRequestTimeout,
      bqWriteRequestSizeLimit,
      goodSinkConcurrency
    )
    private val badProducerBatchSize: Long                 = 8L
    private val badProducerDelayThreshold: FiniteDuration  = FiniteDuration(2L, SECONDS)
    private val badSinkConcurrency: Int                    = 64
    private val sinkSettingsBad                            = SinkSettings.Bad(badProducerBatchSize, badProducerDelayThreshold, badSinkConcurrency)
    private val observedTypesThreshold: Int                = 10
    private val observedTypesTimeout: FiniteDuration       = FiniteDuration(30L, SECONDS)
    private val typeProducerBatchSize: Long                = 4L
    private val typeProducerDelayThreshold: FiniteDuration = FiniteDuration(200L, MILLISECONDS)
    private val typeSinkConcurrency: Int                   = 64
    private val sinkSettingsTypes = SinkSettings.Types(
      observedTypesThreshold,
      observedTypesTimeout,
      typeProducerBatchSize,
      typeProducerDelayThreshold,
      typeSinkConcurrency
    )
    private val failedInsertProducerBatchSize: Long        = 8L
    private val failedInsertDelayThreshold: FiniteDuration = FiniteDuration(2L, SECONDS)
    private val sinkSettingsFailedInserts =
      SinkSettings.FailedInserts(failedInsertProducerBatchSize, failedInsertDelayThreshold)
    private val sinkSettings = SinkSettings(
      sinkSettingsGood,
      sinkSettingsBad,
      sinkSettingsTypes,
      sinkSettingsFailedInserts
    )
    private val initialDelay    = 1
    private val delayMultiplier = 1.5
    private val maxDelay        = 32
    private val totalTimeout    = 5
    private val retrySettings   = BigQueryRetrySettings(initialDelay, delayMultiplier, maxDelay, totalTimeout)

    private val terminationTimeout = FiniteDuration(60, SECONDS)

    private val loader: Config.Loader =
      Config.Loader(lInput, lOutput, loadMode, consumerSettings, sinkSettings, retrySettings, terminationTimeout)

    private val mSubscription: String   = "mutator-sub"
    private val mInput: Input.PubSub    = Input.PubSub(mSubscription)
    private val mOutput: MutatorOutput  = MutatorOutput(good)
    private val mutator: Config.Mutator = Config.Mutator(mInput, mOutput)

    private val rSubscription: String     = "failed-inserts-sub"
    private val rInput: Input.PubSub      = Input.PubSub(rSubscription)
    private val deadLetters: Output.Gcs   = Output.Gcs("gs://some-bucket/")
    private val rOutput: RepeaterOutputs  = RepeaterOutputs(good, deadLetters)
    private val repeater: Config.Repeater = Config.Repeater(rInput, rOutput)

    private val projectId: String = "snowplow-data"

    private val hostname: String          = "statsd.acme.gl"
    private val port: Int                 = 1024
    private val tags: Map[String, String] = Map.empty
    private val period: FiniteDuration    = FiniteDuration(1L, SECONDS)
    private val prefix: Option[String]    = Some("snowplow.monitoring")
    private val statsd: Statsd            = Statsd(hostname, port, tags, period, prefix)
    private val stdout: Stdout            = Stdout(period, prefix)
    private val sentry: SentryConfig      = SentryConfig(URI.create("http://sentry.acme.com"))
    private val dropwizard: Dropwizard    = Dropwizard(period)
    private val monitoring: Monitoring    = Monitoring(Some(statsd), Some(stdout), Some(sentry), Some(dropwizard))

    private[bigquery] val loaderEnv: LoaderEnvironment = Environment(loader, validResolverJson, projectId, monitoring)
    private[bigquery] val mutatorEnv: MutatorEnvironment =
      Environment(mutator, validResolverJson, projectId, monitoring)
    private[bigquery] val repeaterEnv: RepeaterEnvironment =
      Environment(repeater, validResolverJson, projectId, monitoring)
  }
}
