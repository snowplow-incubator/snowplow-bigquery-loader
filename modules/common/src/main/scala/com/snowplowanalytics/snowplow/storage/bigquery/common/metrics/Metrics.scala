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
package com.snowplowanalytics.snowplow.storage.bigquery.common.metrics

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring

import cats.{Applicative, Monad}
import cats.effect.{Async, Clock, Ref, Resource, Sync, Temporal}
import cats.implicits._
import fs2.Stream

import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

sealed trait Metrics[F[_]] {

  /** Send latest metrics to reporter. */
  def report: Stream[F, Unit]

  /** Track latency between collector hit (in millis) and loading time. */
  def latency(collectorTstamp: Long): F[Unit]

  /** Increment the count of events that were successfully loaded into BigQuery. */
  def goodCount(n: Int): F[Unit]

  /** Increment the count of events that failed to be inserted into BigQuery. */
  def failedInsertCount(n: Int): F[Unit]

  /** Increment the count of events that failed validation against their schema. */
  def badCount: F[Unit]

  /** Increment the count of types sent to mutator */
  def typesCount(n: Int): F[Unit]

  /** Increment the count of failed inserts that ultimately could not be loaded into BigQuery. */
  def uninsertableCount(n: Int): F[Unit]

  /** Increment the count of events that inserted by repeater */
  def repeaterInsertedCount: F[Unit]
}

object Metrics {

  private val DefaultPrefix = "snowplow.bigquery.streamloader"

  trait Reporter[F[_]] {
    def report(snapshot: MetricsSnapshot): F[Unit]
  }

  sealed trait MetricsSnapshot
  object MetricsSnapshot {
    final case class LoaderMetricsSnapshot(
      latency: Option[Long],
      goodCount: Int,
      failedInsertCount: Int,
      badCount: Int,
      typesCount: Int
    ) extends MetricsSnapshot

    final case class RepeaterMetricsSnapshot(uninsertableCount: Int, insertedCount: Int) extends MetricsSnapshot
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
    typesCount: Ref[F, Int],
    uninsertableCount: Ref[F, Int],
    repeaterInsertedCount: Ref[F, Int]
  )
  private object MetricsRefs {
    import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.MetricsSnapshot._

    def init[F[_]: Sync]: F[MetricsRefs[F]] =
      for {
        latency               <- Ref.of[F, Option[Long]](None)
        goodCount             <- Ref.of[F, Int](0)
        failedInsertCount     <- Ref.of[F, Int](0)
        badCount              <- Ref.of[F, Int](0)
        typesCount            <- Ref.of[F, Int](0)
        uninsertableCount     <- Ref.of[F, Int](0)
        repeaterInsertedCount <- Ref.of[F, Int](0)
      } yield MetricsRefs(
        latency,
        goodCount,
        failedInsertCount,
        badCount,
        typesCount,
        uninsertableCount,
        repeaterInsertedCount
      )

    def snapshot[F[_]: Monad](refs: MetricsRefs[F], app: ReportingApp): F[MetricsSnapshot] =
      for {
        maybeLatency          <- refs.latency.getAndSet(None)
        goodCount             <- refs.goodCount.getAndSet(0)
        failedInsertCount     <- refs.failedInsertCount.getAndSet(0)
        badCount              <- refs.badCount.getAndSet(0)
        typesCount            <- refs.typesCount.getAndSet(0)
        uninsertableCount     <- refs.uninsertableCount.getAndSet(0)
        repeaterInsertedCount <- refs.repeaterInsertedCount.getAndSet(0)
      } yield app match {
        case ReportingApp.StreamLoader =>
          LoaderMetricsSnapshot(maybeLatency, goodCount, failedInsertCount, badCount, typesCount)
        case ReportingApp.Repeater => RepeaterMetricsSnapshot(uninsertableCount, repeaterInsertedCount)
      }
  }

  def build[F[_]: Async: Logger](
    monitoringConfig: Monitoring,
    app: ReportingApp
  ): F[Metrics[F]] =
    monitoringConfig match {
      case Monitoring(None, None, _, _) => noop[F].pure[F]
      case Monitoring(statsd, stdout, _, _) =>
        (MetricsRefs.init[F], MetricsRefs.init[F]).mapN { (statsdRefs, stdoutRefs) =>
          new Metrics[F] {
            def report: Stream[F, Unit] = {
              val rep1 = statsd
                .map { config =>
                  reporterStream(StatsDReporter.make[F](config), statsdRefs, config.period, app)
                }
                .getOrElse(Stream.never[F])

              val rep2 = stdout
                .map { config =>
                  reporterStream(StdoutReporter.make[F](config), stdoutRefs, config.period, app)
                }
                .getOrElse(Stream.never[F])

              rep1.concurrently(rep2)
            }

            def latency(collectorTstamp: Long): F[Unit] =
              for {
                now <- Clock[F].realTime
                _   <- statsdRefs.latency.set(Some(now.toMillis - collectorTstamp))
                _   <- stdoutRefs.latency.set(Some(now.toMillis - collectorTstamp))
              } yield ()

            def goodCount(n: Int): F[Unit] =
              statsdRefs.goodCount.update(_ + n) *> stdoutRefs.goodCount.update(_ + n)

            def failedInsertCount(n: Int): F[Unit] =
              statsdRefs.failedInsertCount.update(_ + n) *> stdoutRefs.failedInsertCount.update(_ + n)

            def badCount: F[Unit] =
              statsdRefs.badCount.update(_ + 1) *> stdoutRefs.badCount.update(_ + 1)

            def typesCount(n: Int): F[Unit] =
              statsdRefs.typesCount.update(_ + n) *> stdoutRefs.typesCount.update(_ + n)

            def uninsertableCount(n: Int): F[Unit] =
              statsdRefs.uninsertableCount.update(_ + n) *> stdoutRefs.uninsertableCount.update(_ + n)

            def repeaterInsertedCount: F[Unit] =
              statsdRefs.repeaterInsertedCount.update(_ + 1) *> stdoutRefs.repeaterInsertedCount.update(_ + 1)
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
      def typesCount(n: Int): F[Unit]             = Applicative[F].unit
      def uninsertableCount(n: Int): F[Unit]      = Applicative[F].unit
      def repeaterInsertedCount: F[Unit]          = Applicative[F].unit
    }

  private def reporterStream[F[_]: Temporal](
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

  def normalizeMetric(prefix: Option[String], metric: String): String =
    s"${prefix.getOrElse(DefaultPrefix).stripSuffix(".")}.$metric".stripPrefix(".")
}
