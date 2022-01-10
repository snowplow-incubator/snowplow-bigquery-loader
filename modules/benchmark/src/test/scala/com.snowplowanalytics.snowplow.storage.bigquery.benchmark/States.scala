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
package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers

import org.openjdk.jmh.annotations.{Scope, State}

object States {
  @State(Scope.Benchmark)
  class ExampleEventState {
    val baseEvent        = SpecHelpers.events.exampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
    val adClickSchemaKey = SpecHelpers.iglu.adClickSchemaKey
    val unstruct         = baseEvent.copy(unstruct_event = SpecHelpers.events.adClickUnstructEvent)
    val contexts         = SpecHelpers.events.geoContexts
    val resolver         = SpecHelpers.iglu.resolver
    val processor        = SpecHelpers.meta.processor
    val idClock          = SpecHelpers.implicits.idClock
  }
}
