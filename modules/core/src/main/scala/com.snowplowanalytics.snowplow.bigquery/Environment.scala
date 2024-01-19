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
import cats.effect.{Async, Resource, Sync}
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.blaze.client.BlazeClientBuilder
import io.sentry.Sentry

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.bigquery.processing.{BigQueryHealth, BigQueryUtils, TableManager, WriterProvider}
import com.snowplowanalytics.snowplow.runtime.{AppInfo, HealthProbe}

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  tableManager: TableManager[F],
  writerProvider: Resource[F, WriterProvider[F]],
  metrics: Metrics[F],
  batching: Config.Batching,
  badRowMaxSize: Int
)

object Environment {

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config.WithIglu[SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.main.monitoring.sentry)
      bigqueryHealth <- Resource.eval(BigQueryHealth.initUnhealthy[F])
      sourceAndAck <- Resource.eval(toSource(config.main.input))
      _ <- HealthProbe.resource(
             config.main.monitoring.healthProbe.port,
             AppHealth.isHealthy(config.main.monitoring.healthProbe, sourceAndAck, bigqueryHealth)
           )
      resolver <- mkResolver[F](config.iglu)
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      monitoring <- Monitoring.create[F](config.main.monitoring.webhook, appInfo, httpClient)
      badSink <- toSink(config.main.output.bad.sink)
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics))
      creds <- Resource.eval(BigQueryUtils.credentials(config.main.output.good))
      tableManager <- Resource.eval(TableManager.make(config.main.output.good, config.main.retries, creds, bigqueryHealth, monitoring))
      writerProvider <- WriterProvider.make(config.main.output.good, config.main.retries, creds, bigqueryHealth, monitoring)
    } yield Environment(
      appInfo        = appInfo,
      source         = sourceAndAck,
      badSink        = badSink,
      resolver       = resolver,
      httpClient     = httpClient,
      tableManager   = tableManager,
      writerProvider = writerProvider,
      metrics        = metrics,
      batching       = config.main.batching,
      badRowMaxSize  = config.main.output.bad.maxRecordSize
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

}
