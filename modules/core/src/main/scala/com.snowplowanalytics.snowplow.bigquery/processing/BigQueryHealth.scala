/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.{Concurrent, Ref}
import cats.implicits._
import com.snowplowanalytics.snowplow.runtime.HealthProbe

final case class BigQueryHealth[F[_]](state: Ref[F, HealthProbe.Status]) {
  import BigQueryHealth._
  def setUnhealthy(): F[Unit] = state.set(unhealthy)
  def setHealthy(): F[Unit]   = state.set(HealthProbe.Healthy)
}

object BigQueryHealth {
  private val unhealthy = HealthProbe.Unhealthy("BigQuery connection is not healthy")

  def initUnhealthy[F[_]: Concurrent]: F[BigQueryHealth[F]] =
    Ref
      .of[F, HealthProbe.Status](unhealthy)
      .map(BigQueryHealth.apply)
}
