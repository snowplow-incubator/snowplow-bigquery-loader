/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import org.http4s.client.Client
import io.sentry.Sentry
import retry.RetryPolicy

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.bigquery.processing.{BigQueryRetrying, BigQueryUtils, TableManager, Writer}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Webhook}

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  tableManager: TableManager.WithHandledErrors[F],
  writers: Vector[Writer.Provider[F]],
  metrics: Metrics[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  alterTableWaitPolicy: RetryPolicy[F],
  batching: Config.Batching,
  badRowMaxSize: Int,
  schemasToSkip: List[SchemaCriterion],
  legacyColumns: List[SchemaCriterion],
  legacyColumnMode: Boolean,
  exitOnMissingIgluSchema: Boolean
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
      sourceAndAck <- Resource.eval(toSource(config.main.input))
      sourceReporter = sourceAndAck.isHealthy(config.main.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      resolver <- mkResolver[F](config.iglu)
      httpClient <- HttpClient.resource[F](config.main.http.client)
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, appHealth)
      _ <- Webhook.resource(config.main.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <-
        toSink(config.main.output.bad.sink).onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics, sourceAndAck))
      creds <- Resource.eval(BigQueryUtils.credentials(config.main.output.good))
      tableManager <- Resource.eval(TableManager.make(config.main.output.good, creds))
      tableManagerWrapped <- Resource.eval(TableManager.withHandledErrors(tableManager, config.main.retries, appHealth))
      writerBuilder <- Writer.builder(config.main.output.good, creds)
      writerProviders <- Vector.range(0, config.main.batching.writeBatchConcurrency).traverse { _ =>
                           Writer.provider(writerBuilder, config.main.retries, appHealth)
                         }
    } yield Environment(
      appInfo                 = appInfo,
      source                  = sourceAndAck,
      badSink                 = badSink,
      resolver                = resolver,
      httpClient              = httpClient,
      tableManager            = tableManagerWrapped,
      writers                 = writerProviders,
      metrics                 = metrics,
      appHealth               = appHealth,
      alterTableWaitPolicy    = BigQueryRetrying.policyForAlterTableWait[F](config.main.retries),
      batching                = config.main.batching,
      badRowMaxSize           = config.main.output.bad.maxRecordSize,
      schemasToSkip           = config.main.skipSchemas,
      legacyColumns           = config.main.legacyColumns,
      legacyColumnMode        = config.main.legacyColumnMode,
      exitOnMissingIgluSchema = config.main.exitOnMissingIgluSchema
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
