package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

import com.snowplowanalytics.snowplow.storage.bigquery.loader.{LoaderRow, SpecHelpers}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class FromEventBenchmark {
  @Benchmark
  def fromEvent(state: States.ExampleEventState): Unit = {
    LoaderRow.fromEvent(state.resolver)(state.event)
  }
}

object States {
  @State(Scope.Benchmark)
  class ExampleEventState {
    val event = SpecHelpers.ExampleEvent.copy(br_cookies = Some(false), domain_sessionidx = Some(3))
    val resolver = SpecHelpers.resolver
  }
}
