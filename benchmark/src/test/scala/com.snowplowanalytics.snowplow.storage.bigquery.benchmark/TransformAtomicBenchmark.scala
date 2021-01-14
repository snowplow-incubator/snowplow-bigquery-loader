package com.snowplowanalytics.snowplow.storage.bigquery.benchmark

import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderRow
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State}

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class TransformAtomicBenchmark {
  @Benchmark
  def transformAtomic(state: States.ExampleEventState): Unit = {
    LoaderRow.transformAtomic(state.unstruct)
  }
}
