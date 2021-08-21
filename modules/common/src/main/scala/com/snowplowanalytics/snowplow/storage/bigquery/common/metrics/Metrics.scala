/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.common.metrics

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring.Statsd

import cats.{Applicative, Monad}
import cats.effect.{Async, Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.Stream

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

sealed trait Metrics[F[_]] {

  /** Send latest metrics to reporter. */
  def report: Stream[F, Unit]

  /** Track latency between collector hit and loading time. */
  def latency(collectorTstamp: Long): F[Unit]

  /** Increment the count of events that were successfully loaded into BigQuery. */
  def goodCount(n: Int): F[Unit]

  /** Increment the count of events that failed to be inserted into BigQuery. */
  def failedInsertCount(n: Int): F[Unit]

  /** Increment the count of events that failed validation against their schema. */
  def badCount: F[Unit]

  /** Increment the count of failed inserts that ultimately could not be loaded into BigQuery. */
  def uninsertableCount(n: Int): F[Unit]
}

object Metrics {
  trait Reporter[F[_]] {
    def report(snapshot: MetricsSnapshot): F[Unit]
  }

  sealed trait MetricsSnapshot
  object MetricsSnapshot {
    final case class LoaderMetricsSnapshot(
      latency: Option[Long],
      goodCount: Int,
      failedInsertCount: Int,
      badCount: Int
    ) extends MetricsSnapshot

    final case class RepeaterMetricsSnapshot(uninsertableCount: Int) extends MetricsSnapshot
  }

  sealed trait ReportingApp
  object ReportingApp {
    final case object StreamLoader extends ReportingApp
    final case object Repeater extends ReportingApp
  }

  final private case class MetricsRefs[F[_]](
    latency: Ref[F, Option[Long]],
    goodCount: Ref[F, Int],
    failedInsertCount: Ref[F, Int],
    badCount: Ref[F, Int],
    uninsertableCount: Ref[F, Int]
  )
  private object MetricsRefs {
    import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.MetricsSnapshot._

    def init[F[_]: Sync]: F[MetricsRefs[F]] =
      for {
        latency           <- Ref.of[F, Option[Long]](None)
        goodCount         <- Ref.of[F, Int](0)
        failedInsertCount <- Ref.of[F, Int](0)
        badCount          <- Ref.of[F, Int](0)
        uninsertableCount <- Ref.of[F, Int](0)
      } yield MetricsRefs(latency, goodCount, failedInsertCount, badCount, uninsertableCount)

    def snapshot[F[_]: Monad](refs: MetricsRefs[F], app: ReportingApp): F[MetricsSnapshot] =
      for {
        maybeLatency      <- refs.latency.getAndSet(None)
        goodCount         <- refs.goodCount.getAndSet(0)
        failedInsertCount <- refs.failedInsertCount.getAndSet(0)
        badCount          <- refs.badCount.getAndSet(0)
        uninsertableCount <- refs.uninsertableCount.getAndSet(0)
      } yield app match {
        case ReportingApp.StreamLoader => LoaderMetricsSnapshot(maybeLatency, goodCount, failedInsertCount, badCount)
        case ReportingApp.Repeater     => RepeaterMetricsSnapshot(uninsertableCount)
      }
  }

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    monitoringConfig: Option[Statsd],
    app: ReportingApp
  ): F[Metrics[F]] =
    monitoringConfig match {
      case None => noop[F].pure[F]
      case Some(statsd) =>
        MetricsRefs.init[F].map { refs =>
          new Metrics[F] {
            def report: Stream[F, Unit] =
              reporterStream(StatsDReporter.make[F](blocker, monitoringConfig), refs, statsd.period, app)

            def latency(collectorTstamp: Long): F[Unit] =
              for {
                now <- Clock[F].realTime(MILLISECONDS)
                _   <- refs.latency.set(Some(now - collectorTstamp))
              } yield ()

            def goodCount(n: Int): F[Unit] = refs.goodCount.update(_ + n)

            def failedInsertCount(n: Int): F[Unit] = refs.failedInsertCount.update(_ + n)

            def badCount: F[Unit] = refs.badCount.update(_ + 1)

            def uninsertableCount(n: Int): F[Unit] = refs.uninsertableCount.update(_ + n)
          }
        }
    }

  def noop[F[_]: Async]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit]                 = Stream.never[F]
      def latency(collectorTstamp: Long): F[Unit] = Applicative[F].unit
      def goodCount(n: Int): F[Unit]              = Applicative[F].unit
      def failedInsertCount(n: Int): F[Unit]      = Applicative[F].unit
      def badCount: F[Unit]                       = Applicative[F].unit
      def uninsertableCount(n: Int): F[Unit]      = Applicative[F].unit
    }

  private def reporterStream[F[_]: Sync: Timer: ContextShift](
    reporter: Resource[F, Reporter[F]],
    metrics: MetricsRefs[F],
    period: FiniteDuration,
    app: ReportingApp
  ): Stream[F, Unit] =
    for {
      rep      <- Stream.resource(reporter)
      _        <- Stream.fixedDelay[F](period)
      snapshot <- Stream.eval(MetricsRefs.snapshot(metrics, app))
      _        <- Stream.eval(rep.report(snapshot))
    } yield ()
}
