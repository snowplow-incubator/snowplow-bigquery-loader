/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.bigquery

import cats.Id
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{ExitCode, IO}
import com.comcast.ip4s.Port
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.bigquery.Config.GcpUserAgent
import com.snowplowanalytics.snowplow.pubsub.{GcpUserAgent => PubsubUserAgent}
import com.snowplowanalytics.snowplow.runtime.Metrics.StatsdConfig
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser, HttpClient, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.snowplow.sinks.pubsub.PubsubSinkConfig
import com.snowplowanalytics.snowplow.sources.pubsub.PubsubSourceConfig
import org.http4s.implicits.http4sLiteralsSyntax
import org.specs2.Specification

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt

class PubsubConfigSpec extends Specification with CatsEffect {

  def is = s2"""
   Config parse should be able to parse
    minimal pubsub config $minimal
    extended pubsub config $extended
  """

  private def minimal =
    assert(
      resource = "/config.pubsub.minimal.hocon",
      expectedResult = Right(
        PubsubConfigSpec.minimalConfig
      )
    )

  private def extended =
    assert(
      resource = "/config.pubsub.reference.hocon",
      expectedResult = Right(
        PubsubConfigSpec.extendedConfig
      )
    )

  private def assert(resource: String, expectedResult: Either[ExitCode, Config[PubsubSourceConfig, PubsubSinkConfig]]) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[PubsubSourceConfig, PubsubSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

object PubsubConfigSpec {
  private val minimalConfig = Config[PubsubSourceConfig, PubsubSinkConfig](
    input = PubsubSourceConfig(
      subscription               = PubsubSourceConfig.Subscription("my-project", "snowplow-enriched"),
      parallelPullFactor         = BigDecimal(0.5),
      bufferMaxBytes             = 10000000,
      maxAckExtensionPeriod      = 1.hour,
      minDurationPerAckExtension = 1.minute,
      maxDurationPerAckExtension = 10.minutes,
      gcpUserAgent               = PubsubUserAgent("Snowplow OSS", "bigquery-loader"),
      shutdownTimeout            = 30.seconds
    ),
    output = Config.Output(
      good = Config.BigQuery(
        project      = "my-project",
        dataset      = "my-dataset",
        table        = "events",
        gcpUserAgent = GcpUserAgent(productName = "Snowplow OSS"),
        credentials  = None
      ),
      bad = Config.SinkWithMaxSize(
        sink = PubsubSinkConfig(
          topic                = PubsubSinkConfig.Topic("my-project", "snowplow-bad"),
          batchSize            = 1000L,
          requestByteThreshold = 1000000L,
          gcpUserAgent         = PubsubUserAgent("Snowplow OSS", "bigquery-loader")
        ),
        maxRecordSize = 10000000
      )
    ),
    batching = Config.Batching(
      maxBytes              = 16000000,
      maxDelay              = 1.second,
      writeBatchConcurrency = 3
    ),
    retries = Config.Retries(
      setupErrors     = Retrying.Config.ForSetup(delay = 30.seconds),
      transientErrors = Retrying.Config.ForTransient(delay = 1.second, attempts = 5),
      alterTableWait  = Config.AlterTableWaitRetries(delay = 1.second),
      tooManyColumns  = Config.TooManyColumnsRetries(delay = 300.seconds)
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      interval        = 15.minutes,
      collectorUri    = "collector-g.snowplowanalytics.com",
      collectorPort   = 443,
      secure          = true,
      userProvidedId  = None,
      autoGeneratedId = None,
      instanceId      = None,
      moduleName      = None,
      moduleVersion   = None
    ),
    monitoring = Config.Monitoring(
      metrics     = Config.Metrics(None),
      sentry      = None,
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 5.minutes),
      webhook     = Webhook.Config(endpoint = None, tags = Map.empty, heartbeat = 60.minutes)
    ),
    license                 = AcceptedLicense(),
    skipSchemas             = List.empty,
    legacyColumns           = List.empty,
    exitOnMissingIgluSchema = true,
    http                    = Config.Http(HttpClient.Config(4))
  )

  private val extendedConfig = Config[PubsubSourceConfig, PubsubSinkConfig](
    input = PubsubSourceConfig(
      subscription               = PubsubSourceConfig.Subscription("my-project", "snowplow-enriched"),
      parallelPullFactor         = BigDecimal(0.5),
      bufferMaxBytes             = 1000000,
      maxAckExtensionPeriod      = 1.hour,
      minDurationPerAckExtension = 1.minute,
      maxDurationPerAckExtension = 10.minutes,
      gcpUserAgent               = PubsubUserAgent("Snowplow OSS", "bigquery-loader"),
      shutdownTimeout            = 30.seconds
    ),
    output = Config.Output(
      good = Config.BigQuery(
        project      = "my-project",
        dataset      = "my-dataset",
        table        = "events",
        gcpUserAgent = GcpUserAgent(productName = "Snowplow OSS"),
        credentials  = None
      ),
      bad = Config.SinkWithMaxSize(
        sink = PubsubSinkConfig(
          topic                = PubsubSinkConfig.Topic("my-project", "snowplow-bad"),
          batchSize            = 100L,
          requestByteThreshold = 1000000L,
          gcpUserAgent         = PubsubUserAgent("Snowplow OSS", "bigquery-loader")
        ),
        maxRecordSize = 10000000
      )
    ),
    batching = Config.Batching(
      maxBytes              = 16000000,
      maxDelay              = 1.second,
      writeBatchConcurrency = 1
    ),
    retries = Config.Retries(
      setupErrors     = Retrying.Config.ForSetup(delay = 30.seconds),
      transientErrors = Retrying.Config.ForTransient(delay = 1.second, attempts = 5),
      alterTableWait  = Config.AlterTableWaitRetries(delay = 1.second),
      tooManyColumns  = Config.TooManyColumnsRetries(delay = 300.seconds)
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      interval        = 15.minutes,
      collectorUri    = "collector-g.snowplowanalytics.com",
      collectorPort   = 443,
      secure          = true,
      userProvidedId  = Some("my_pipeline"),
      autoGeneratedId = Some("hfy67e5ydhtrd"),
      instanceId      = Some("665bhft5u6udjf"),
      moduleName      = Some("bigquery-loader-vmss"),
      moduleVersion   = Some("1.0.0")
    ),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          StatsdConfig(
            hostname = "127.0.0.1",
            port     = 8125,
            tags     = Map("myTag" -> "xyz"),
            period   = 1.minute,
            prefix   = "snowplow.bigquery.loader"
          )
        )
      ),
      sentry = Some(Config.SentryM[Id](dsn = "https://public@sentry.example.com/1", tags = Map("myTag" -> "xyz"))),
      healthProbe = Config.HealthProbe(
        port             = Port.fromInt(8000).get,
        unhealthyLatency = 5.minutes
      ),
      webhook =
        Webhook.Config(endpoint = Some(uri"https://webhook.acme.com"), tags = Map("pipeline" -> "production"), heartbeat = 60.minutes)
    ),
    license = AcceptedLicense(),
    skipSchemas = List(
      SchemaCriterion.parse("iglu:com.acme/skipped1/jsonschema/1-0-0").get,
      SchemaCriterion.parse("iglu:com.acme/skipped2/jsonschema/1-0-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped3/jsonschema/1-*-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped4/jsonschema/*-*-*").get
    ),
    legacyColumns = List(
      SchemaCriterion.parse("iglu:com.acme/legacy/jsonschema/1-*-*").get,
      SchemaCriterion.parse("iglu:com.acme/legacy/jsonschema/2-*-*").get
    ),
    exitOnMissingIgluSchema = true,
    http                    = Config.Http(HttpClient.Config(4))
  )
}
