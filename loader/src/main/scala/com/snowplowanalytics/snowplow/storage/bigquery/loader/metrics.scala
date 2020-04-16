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

import scala.util.control.Exception

object metrics {
  val metrics = new MetricRegistry()
  val latency = new MetricRegistryOps(metrics)
  val logger = LoggerFactory.getLogger("bigquery.loader.metrics")
  // Take the latest value every 100ms.
  val reporter = Slf4jReporter.forRegistry(metrics).outputTo(logger).build().start(100, TimeUnit.MILLISECONDS)

  /** Operations performed on a [[MetricRegistry]] */
  final class MetricRegistryOps(registry: MetricRegistry) {
    // GaugeWithValue registers a fresh Gauge metric every time, so first we have to clean up the registry.
    def gauge[T](name: String, f: => T): Gauge[T] = {
      registry.remove(name)
      registry.register(name, GaugeWithValue(f))
    }

    // `getValue` can throw an exception when multiple workers try to
    // register the same metric.
    def update(diff: Long): Either[Throwable, Long] = Exception.nonFatalCatch.either(this.gauge("bigquery.loader.latency", diff).getValue)
  }

  /** The default [[Gauge]] has no way to pass in a value. */
  final object GaugeWithValue {
    def apply[T](f: => T): Gauge[T] = new Gauge[T] { def getValue: T = f }
  }
}
