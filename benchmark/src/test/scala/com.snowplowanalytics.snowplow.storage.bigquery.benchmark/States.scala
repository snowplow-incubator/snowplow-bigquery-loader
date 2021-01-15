/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import org.openjdk.jmh.annotations.{Scope, State}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.UnstructEvent
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers

object States {
  @State(Scope.Benchmark)
  class ExampleEventState {
    val baseEvent = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
    val adClickSchemaKey =
      SchemaKey("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1, 0, 0))
    val adClickJson = SpecHelpers.adClick
    val unstruct =
      baseEvent.copy(unstruct_event = UnstructEvent(Some(SelfDescribingData(adClickSchemaKey, adClickJson))))
  }
}
