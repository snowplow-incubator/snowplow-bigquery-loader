package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.UnstructEvent
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers
import org.openjdk.jmh.annotations.{Scope, State}

object States {
  @State(Scope.Benchmark)
  class ExampleEventState {
    val baseEvent = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
    val adClickSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ad_click", "jsonschema", SchemaVer.Full(1,0,0))
    val adClickJson = SpecHelpers.adClick
    val unstruct = baseEvent.copy(unstruct_event = UnstructEvent(Some(SelfDescribingData(adClickSchemaKey, adClickJson))))
  }
}
