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
package com.snowplowanalytics.snowplow.storage.bigquery.streamloader.metrics

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
}

object Metrics {
  trait Reporter[F[_]] {
    def report(latency: Option[Long]): F[Unit]
  }

  // A ref of when event was last processed, and its collector timestamp
  final case class LatencyRef[F[_]](timestamps: Ref[F, Option[(Long, Long)]])
  object LatencyRef {
    def init[F[_]: Sync]: F[LatencyRef[F]] = Ref.of[F, Option[(Long, Long)]](None).map(LatencyRef(_))
    def snapshot[F[_]: Monad](ref: LatencyRef[F], minTime: Long): F[Option[Long]] = ref.timestamps.get.map {
      case Some((loadTstamp, collectorTstamp)) if loadTstamp > minTime => Some(loadTstamp - collectorTstamp)
      case _                                                           => None
    }
  }

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    monitoringConfig: Option[Statsd]
  ): F[Metrics[F]] =
    monitoringConfig match {
      case None => noop[F].pure[F]
      case Some(_) =>
        LatencyRef.init[F].map { ref =>
          new Metrics[F] {
            def report: Stream[F, Unit] =
              monitoringConfig
                .map { mc =>
                  reporterStream(StatsDReporter.make[F](blocker, monitoringConfig), ref, mc.period)
                }
                .getOrElse(Stream.never[F])
            def latency(collectorTstamp: Long): F[Unit] =
              for {
                now <- Clock[F].realTime(MILLISECONDS)
                _   <- ref.timestamps.set(Some(now -> collectorTstamp))
              } yield ()
          }
        }
    }

  def noop[F[_]: Async]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit]                 = Stream.never[F]
      def latency(collectorTstamp: Long): F[Unit] = Applicative[F].unit
    }

  private def reporterStream[F[_]: Sync: Timer: ContextShift](
    reporter: Resource[F, Reporter[F]],
    latency: LatencyRef[F],
    period: FiniteDuration
  ): Stream[F, Unit] =
    for {
      rep           <- Stream.resource(reporter)
      lastReportRef <- Stream.eval(Clock[F].realTime(MILLISECONDS)).evalMap(Ref.of(_))
      _             <- Stream.fixedDelay[F](period)
      lastReport    <- Stream.eval(Clock[F].realTime(MILLISECONDS)).evalMap(lastReportRef.getAndSet)
      snapshot      <- Stream.eval(LatencyRef.snapshot(latency, lastReport))
      _             <- Stream.eval(rep.report(snapshot))
    } yield ()
}
