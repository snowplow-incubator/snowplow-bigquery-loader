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

import cats.effect.{Resource, Sync}

import org.typelevel.log4cats.Logger

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring.Stdout

object StdoutReporter {

  def make[F[_]: Logger: Sync](config: Stdout): Resource[F, Metrics.Reporter[F]] =
    Resource.eval[F, Metrics.Reporter[F]](Sync[F].delay(new Metrics.Reporter[F] {
      def report(snapshot: Metrics.MetricsSnapshot): F[Unit] =
        snapshot match {
          case lms: Metrics.MetricsSnapshot.LoaderMetricsSnapshot =>
            val totalEventCount = lms.goodCount + lms.badCount + lms.failedInsertCount
            val logPeriod= s"${Metrics.normalizeMetric(config.prefix, "StatisticsPeriod")} = ${config.period}"
            val total = s"TotalEvent = $totalEventCount"
            val good = s"GoodEvent = ${lms.goodCount}"
            val failedInsert = s"FailedInsert = ${lms.failedInsertCount}"
            val badEvent = s"BadEvent = ${lms.badCount}"
            val types = s"Types = ${lms.typesCount}"
            Logger[F].info(s"$logPeriod, $total, $good, $failedInsert, $badEvent, $types")
          case rms: Metrics.MetricsSnapshot.RepeaterMetricsSnapshot =>
            val logPeriod = s"${Metrics.normalizeMetric(config.prefix, "Statistics")} = ${config.period}"
            val uninsertable = s"UninsertableEvents = ${rms.uninsertableCount}"
            val inserted = s"InsertedEvents = ${rms.insertedCount}"
            Logger[F].info(s"$logPeriod, $uninsertable, $inserted")
        }
    }))

}
