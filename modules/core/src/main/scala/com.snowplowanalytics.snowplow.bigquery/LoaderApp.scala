/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.{ExitCode, IO, Resource}
import io.circe.Decoder
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.runtime.AppInfo

abstract class LoaderApp[SourceConfig: Decoder, SinkConfig: Decoder](
  info: AppInfo
) extends CommandIOApp(name = LoaderApp.helpCommand(info), header = info.dockerAlias, version = info.version) {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  type SinkProvider   = SinkConfig => Resource[IO, Sink[IO]]
  type SourceProvider = SourceConfig => IO[SourceAndAck[IO]]

  def source: SourceProvider
  def badSink: SinkProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(info, source, badSink)

}

object LoaderApp {

  private def helpCommand(appInfo: AppInfo) = s"docker run ${appInfo.dockerAlias}"

}
