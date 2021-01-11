package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import com.snowplowanalytics.iglu.client.Resolver
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class FromJsonBenchmark {
  @Benchmark
  def fromJson(state: States.ExampleEventState): Unit = {
    LoaderRow.fromJson(state.resolver)(state.event)
  }
}

object States {
  @State(Scope.Benchmark)
  class ExampleEventState {
    val event = parse("""{
      "collector_tstamp": "2018-09-01T17:48:34.000Z",
      "txn_id": null,
      "geo_location": "12.0,1.0",
      "domain_sessionidx": 3,
      "br_cookies": false
    }""").asInstanceOf[JObject]
    val resolver = Resolver(0, Nil, None)
  }
}
