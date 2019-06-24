/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.time.Instant
import java.util.UUID
import io.circe.literal._
import io.circe.Json
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

object SpecHelpers {

  val reconstructableEvent = json"""
    {
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
    }
  """

  def createContexts(flattenSchemaKey: String): Json = json"""{
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
  }
  """

  def createUnstructEvent(flattenSchemaKey: String): Json = json"""{
    $flattenSchemaKey:
      {
        "chrome_first_paint":1458561380397,
        "connect_end":1458561378602,
        "connect_start":1458561378435
      }
    }
    """

  def minimalEventJson(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Json = json"""
    {
      "collector_tstamp" : $collectorTstamp,
      "event_id":$id,
      "v_collector":$vCollector,
      "v_etl": $vTstamp
    }
    """

  def minimalEvent(id: UUID, collectorTstamp: Instant, vCollector: String, vTstamp: String): Event =
    Event(None, None, None, collectorTstamp, None, None, id, None, None, None, vCollector, vTstamp, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, UnstructEvent(None), None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
      Contexts(Nil), None, None, None, None, None, None, None, None)

  val exampleMinimalEvent = minimalEvent(
    UUID.fromString("ba553b7f-63d5-47ad-8697-06016b472c34"),
    Instant.ofEpochMilli(1550477167580L),
    "bq-loader-test",
    "bq-loader-test"
  )

  val requiredFieldMissingEventJson = json"""
    {
      "collector_tstamp" : "2016-03-21T11:56:32.844Z",
      "event_id" : "ba553b7f-63d5-47ad-8697-06016b472c34",
      "v_collector": "bq-loader-test"
    }
    """

}
