/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.implicits._
import cats.{Functor, Monad, Monoid}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.runtime.HealthProbe.{Healthy, Unhealthy}
import com.snowplowanalytics.snowplow.bigquery.processing.BigQueryHealth
import com.snowplowanalytics.snowplow.sources.SourceAndAck

object AppHealth {

  def isHealthy[F[_]: Monad](
    config: Config.HealthProbe,
    source: SourceAndAck[F],
    bigqueryHealth: BigQueryHealth[F]
  ): F[HealthProbe.Status] =
    List(
      sourceIsHealthy(config, source),
      bigqueryHealth.state.get
    ).foldA

  private def sourceIsHealthy[F[_]: Functor](config: Config.HealthProbe, source: SourceAndAck[F]): F[HealthProbe.Status] =
    source.isHealthy(config.unhealthyLatency).map {
      case SourceAndAck.Healthy              => HealthProbe.Healthy
      case unhealthy: SourceAndAck.Unhealthy => HealthProbe.Unhealthy(unhealthy.show)
    }

  private val combineHealth: (HealthProbe.Status, HealthProbe.Status) => HealthProbe.Status = {
    case (Healthy, Healthy)                    => Healthy
    case (Healthy, unhealthy)                  => unhealthy
    case (unhealthy, Healthy)                  => unhealthy
    case (Unhealthy(first), Unhealthy(second)) => Unhealthy(reason = s"$first, $second")
  }

  private implicit val healthMonoid: Monoid[HealthProbe.Status] = Monoid.instance(Healthy, combineHealth)
}
