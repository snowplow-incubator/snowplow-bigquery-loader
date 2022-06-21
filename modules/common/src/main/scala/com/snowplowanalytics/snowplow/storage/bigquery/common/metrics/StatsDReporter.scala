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

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring.Statsd
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.MetricsSnapshot
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics.MetricsSnapshot.{
  LoaderMetricsSnapshot,
  RepeaterMetricsSnapshot
}

import cats.effect.{Resource, Sync}
import cats.implicits._
import org.typelevel.log4cats.Logger

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

/**
  * Reports metrics to a StatsD server over UDP
  *
  * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the metric, e.g:
  * `snowplow.bigquery.streamloader.latency:2000|g|#app_id:12345,env:prod`
  */
object StatsDReporter {
  private val LatencyGaugeName      = "latency"
  private val GoodCountName         = "good"
  private val FailedInsertCountName = "failed_inserts"
  private val BadCountName          = "bad"
  private val UninsertableName      = "uninsertable"

  /** A reporter which sends metrics from the registry to the StatsD server. */
  def make[F[_]: Sync: Logger](
    config: Statsd
  ): Resource[F, Metrics.Reporter[F]] =
    Resource.fromAutoCloseable(Sync[F].delay(new DatagramSocket)).map(impl[F](config, _))

  /**
    * The stream calls `InetAddress.getByName` each time there is a new batch of metrics. This allows
    * the run-time to resolve the address to a new IP address, in case DNS records change.  This is
    * necessary in dynamic container environments (Kubernetes) where the statsd server could get
    * restarted at a new IP address.
    *
    * Note, InetAddress caches name resolutions, (see https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/InetAddress.html)
    * so there could be a delay in following a DNS record change.  For the Docker image we release
    * the cache time is 30 seconds.
    */
  private def impl[F[_]: Sync: Logger](
    monitoringConfig: Statsd,
    socket: DatagramSocket
  ): Metrics.Reporter[F] =
    new Metrics.Reporter[F] {
      def report(snapshot: MetricsSnapshot): F[Unit] =
        (for {
          inetAddr <- Sync[F].interruptible(InetAddress.getByName(monitoringConfig.hostname))
          _ <- serializeMetrics(snapshot, monitoringConfig).traverse_(
            sendMetric[F](socket, inetAddr, monitoringConfig.port)
          )
        } yield ()).handleErrorWith { t =>
          Logger[F].error(t)("Caught exception sending metrics")
        }
    }

  type KeyValueMetric = (String, String)

  private def serializeMetrics(snapshot: MetricsSnapshot, monitoringConfig: Statsd): List[String] =
    kvs(snapshot).map(statsDFormat(monitoringConfig))

  private def kvs(snapshot: MetricsSnapshot): List[KeyValueMetric] = snapshot match {
    case lms: LoaderMetricsSnapshot =>
      List(
        GoodCountName         -> lms.goodCount.toString,
        FailedInsertCountName -> lms.failedInsertCount.toString,
        BadCountName          -> lms.badCount.toString
      ) ++ lms.latency.map(l => LatencyGaugeName -> l.toString)
    case rms: RepeaterMetricsSnapshot =>
      List(
        UninsertableName -> rms.uninsertableCount.toString
      )
  }

  private def statsDFormat(monitoringConfig: Statsd): KeyValueMetric => String = {
    val tagStr = monitoringConfig.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    kv =>
      val (k, v) = kv
      if (k == LatencyGaugeName) {
        s"${Metrics.normalizeMetric(monitoringConfig.prefix, k)}:${v}|g|#$tagStr"
      } else {
        s"${Metrics.normalizeMetric(monitoringConfig.prefix, k)}:${v}|c|#$tagStr"
      }
  }

  private def sendMetric[F[_]: Sync](
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    metric: String
  ): F[Unit] = {
    val bytes  = metric.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    Sync[F].interruptible(socket.send(packet))
  }
}
