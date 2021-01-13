package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

import com.snowplowanalytics.snowplow.storage.bigquery.loader.{LoaderRow, SpecHelpers}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class TransformJsonBenchmark {
  @Benchmark
  def transformJson(state: States.ExampleEventState): Unit = {
    val selfDesc = state.unstruct.unstruct_event.data.get
    LoaderRow.transformJson(state.resolver, selfDesc.schema)(selfDesc.data)
  }
}
