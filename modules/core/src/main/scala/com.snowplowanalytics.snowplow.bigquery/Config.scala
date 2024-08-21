/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.Id
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import com.comcast.ip4s.Port

import scala.concurrent.duration.FiniteDuration
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  batching: Config.Batching,
  retries: Config.Retries,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion],
  legacyColumns: List[SchemaCriterion]
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: BigQuery, bad: SinkWithMaxSize[Sink])

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class MaxRecordSize(maxRecordSize: Int)

  case class BigQuery(
    project: String,
    dataset: String,
    table: String,
    gcpUserAgent: GcpUserAgent,
    credentials: Option[String]
  )

  case class GcpUserAgent(productName: String)

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration,
    writeBatchConcurrency: Int
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig]
  )

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry],
    healthProbe: HealthProbe,
    webhook: Webhook.Config
  )

  case class AlterTableWaitRetries(delay: FiniteDuration)
  case class TooManyColumnsRetries(delay: FiniteDuration)

  case class Retries(
    setupErrors: Retrying.Config.ForSetup,
    transientErrors: Retrying.Config.ForTransient,
    alterTableWait: AlterTableWaitRetries,
    tooManyColumns: TooManyColumnsRetries
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val userAgent = deriveConfiguredDecoder[GcpUserAgent]
    implicit val bigquery  = deriveConfiguredDecoder[BigQuery]
    implicit val output    = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching  = deriveConfiguredDecoder[Batching]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val alterTableRetries  = deriveConfiguredDecoder[AlterTableWaitRetries]
    implicit val tooManyColsRetries = deriveConfiguredDecoder[TooManyColumnsRetries]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]

    // TODO add bigquery docs
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.0/"))

    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
