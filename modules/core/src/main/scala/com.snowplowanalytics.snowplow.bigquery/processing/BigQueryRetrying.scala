/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Applicative
import cats.effect.Sync
import cats.implicits._
import com.google.api.gax.rpc.PermissionDeniedException
import com.google.cloud.bigquery.BigQueryException
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry._
import retry.implicits.retrySyntaxError

import com.snowplowanalytics.snowplow.bigquery.{Alert, AppHealth, Config, Monitoring}
import com.snowplowanalytics.snowplow.bigquery.processing.BigQueryUtils.BQExceptionSyntax

object BigQueryRetrying {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def withRetries[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert
  )(
    action: F[A]
  ): F[A] =
    retryUntilSuccessful(appHealth, config, monitoring, toAlert, action) <*
      appHealth.setServiceHealth(AppHealth.Service.BigQueryClient, isHealthy = true)

  private def retryUntilSuccessful[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert,
    action: F[A]
  ): F[A] =
    action
      .onError(_ => appHealth.setServiceHealth(AppHealth.Service.BigQueryClient, isHealthy = false))
      .retryingOnSomeErrors(
        isWorthRetrying = isSetupError[F](_),
        policy          = policyForSetupErrors[F](config),
        onError         = logErrorAndSendAlert[F](monitoring, toAlert, _, _)
      )
      .retryingOnAllErrors(
        policy  = policyForTransientErrors[F](config),
        onError = logError[F](_, _)
      )

  private def isSetupError[F[_]: Sync](t: Throwable): F[Boolean] = t match {
    case bqe: BigQueryException if Set("notfound", "accessdenied").contains(bqe.lowerCaseReason) =>
      true.pure[F]
    case _: PermissionDeniedException =>
      true.pure[F]
    case _ =>
      false.pure[F]
  }

  private def policyForSetupErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.exponentialBackoff[F](config.setupErrors.delay)

  private def policyForTransientErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.fullJitter[F](config.transientErrors.delay).join(RetryPolicies.limitRetries(config.transientErrors.attempts - 1))

  def policyForAlterTableWait[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.fibonacciBackoff[F](config.alterTableWait.delay)

  private def logErrorAndSendAlert[F[_]: Sync](
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert,
    error: Throwable,
    details: RetryDetails
  ): F[Unit] =
    Logger[F].error(error)(s"Executing BigQuery command failed. ${extractRetryDetails(details)}") *>
      monitoring.alert(toAlert(error))

  private def logError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    Logger[F].error(error)(s"Executing BigQuery command failed. ${extractRetryDetails(details)}")

  def extractRetryDetails(details: RetryDetails): String = details match {
    case RetryDetails.GivingUp(totalRetries, totalDelay) =>
      s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
    case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
      s"Will retry in ${nextDelay.toSeconds} seconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toSeconds} seconds"
  }
}
