/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.snowplowanalytics.snowplow.runtime.AppInfo
import org.http4s.circe.jsonEncoder
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Method, Request}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait Monitoring[F[_]] {
  def alert(message: Alert): F[Unit]
}

object Monitoring {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def create[F[_]: Sync](
    config: Option[Config.Webhook],
    appInfo: AppInfo,
    httpClient: Client[F]
  )(implicit E: EntityDecoder[F, String]
  ): Resource[F, Monitoring[F]] = Resource.pure {
    new Monitoring[F] {

      override def alert(message: Alert): F[Unit] =
        config match {
          case Some(webhookConfig) =>
            val request = buildHttpRequest(webhookConfig, message)
            executeHttpRequest(webhookConfig, httpClient, request)
          case None =>
            Logger[F].debug("Webhook monitoring is not configured, skipping alert")
        }

      def buildHttpRequest(webhookConfig: Config.Webhook, alert: Alert): Request[F] =
        Request[F](Method.POST, webhookConfig.endpoint)
          .withEntity(Alert.toSelfDescribingJson(alert, appInfo, webhookConfig.tags))

      def executeHttpRequest(
        webhookConfig: Config.Webhook,
        httpClient: Client[F],
        request: Request[F]
      ): F[Unit] =
        httpClient
          .run(request)
          .use { response =>
            if (response.status.isSuccess) Sync[F].unit
            else {
              response.as[String].flatMap(body => Logger[F].error(s"Webhook ${webhookConfig.endpoint} returned non-2xx response:\n$body"))
            }
          }
          .handleErrorWith { e =>
            Logger[F].error(e)(s"Webhook ${webhookConfig.endpoint} resulted in exception without a response")
          }
    }
  }

}
