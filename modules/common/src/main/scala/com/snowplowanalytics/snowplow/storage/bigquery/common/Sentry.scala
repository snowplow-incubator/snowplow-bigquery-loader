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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import cats.effect.{Resource, Sync}

import io.sentry.{Sentry => RSentry, SentryOptions}

import org.typelevel.log4cats.Logger

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring

trait Sentry[F[_]] {
  def trackException(e: Throwable): F[Unit]
}

object Sentry {

  def init[F[_]: Logger: Sync](config: Option[Monitoring.Sentry]): Resource[F, Sentry[F]] =
    config match {
      case Some(Monitoring.Sentry(uri)) =>
        val acquire = Sync[F].delay(RSentry.init(SentryOptions.defaults(uri.toString)))
        Resource
          .make(acquire)(client => Sync[F].delay(client.closeConnection()))
          .map(sentryClient =>
            new Sentry[F] {
              def trackException(e: Throwable): F[Unit] =
                Sync[F].delay(sentryClient.sendException(e))
            }
          )
          .evalTap { _ =>
            Logger[F].info(s"Sentry has been initialised at $uri")
          }

      case None =>
        Resource.pure[F, Sentry[F]](new Sentry[F] {
          def trackException(e: Throwable): F[Unit] =
            Sync[F].unit
        })
    }
}
