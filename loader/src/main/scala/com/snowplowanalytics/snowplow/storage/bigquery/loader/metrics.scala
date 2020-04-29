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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import com.codahale.metrics.{Gauge, MetricRegistry, Slf4jReporter}

import cats.syntax.either._

object metrics {
  private val metrics = new MetricRegistry()
  val latency         = new MetricRegistryOps(metrics)
  private val logger  = LoggerFactory.getLogger("bigquery.loader.metrics")
  // Take the latest value every 1 second.
  private val reporter = Slf4jReporter.forRegistry(metrics).outputTo(logger).build().start(1, TimeUnit.SECONDS)

  /** Operations performed on a `MetricRegistry` */
  final class MetricRegistryOps(registry: MetricRegistry) {
    // A by-name param:
    // - lets us use f directly
    // - ensures we calculate f at the last possible moment
    // - makes it easier to change our mind about f's input
    def gauge[T](name: String, f: => T): Gauge[T] = {
      // GaugeWithValue registers a fresh Gauge metric every time, so first we have to clean up the registry.
      registry.remove(name)
      registry.register(name, GaugeWithValue(f))
    }

    // `getValue` can throw an exception.
    def update(diff: Long): Either[Throwable, Long] =
      Either.catchNonFatal(this.gauge("bigquery.loader.latency", diff).getValue)
  }

  /** The default `Gauge` has no way to pass in a value. */
  final object GaugeWithValue {
    def apply[T](f: => T): Gauge[T] = new Gauge[T] { def getValue: T = f }
  }
}
