/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addGood(count: Long): F[Unit]
  def addBad(count: Long): F[Unit]
  def setLatencyMillis(latencyMillis: Long): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    good: Long,
    bad: Long,
    latencyMillis: Long
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountGood(good),
        KVMetric.CountBad(bad),
        KVMetric.LatencyMillis(latencyMillis)
      )
  }

  private object State {
    def empty: State = State(0, 0, 0L)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, State.empty, config.statsd) with Metrics[F] {
      def addGood(count: Long): F[Unit] =
        ref.update(s => s.copy(good = s.good + count))
      def addBad(count: Long): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def setLatencyMillis(latencyMillis: Long): F[Unit] =
        ref.update(s => s.copy(latencyMillis = s.latencyMillis.max(latencyMillis)))
    }

  private object KVMetric {

    final case class CountGood(v: Long) extends CommonMetrics.KVMetric {
      val key        = "events_good"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Long) extends CommonMetrics.KVMetric {
      val key        = "events_bad"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class LatencyMillis(v: Long) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

  }
}
