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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.{CliConfig, Environment}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.RepeaterEnvironment

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline._

/** Repeater-specific CLI configuration */
object RepeaterCli {
  private val GcsPrefix          = "gs://"
  private val DefaultBufferSize  = 20
  private val DefaultTimeout     = 30
  private val DefaultBackoffTime = 900

  final case class GcsPath(bucket: String, path: String)

  private val options: Opts[RepeaterEnvironment] =
    CliConfig.options.map {
      case CliConfig.Parsed(config, resolver) =>
        Environment(config.repeater, resolver, config.projectId, config.monitoring)
    }

  private val bufferSize = Opts
    .option[Int](
      "bufferSize",
      "Some failed inserts cannot be written to BigQuery, even after multiple attempts. " +
        "These events are buffered and ultimately sunk to GCS." +
        "The buffer is flushed when bufferSize is reached or when timeout passes."
    )
    .validate("Buffer size must be greater than 0.") { _ > 0 }
    .withDefault(DefaultBufferSize)

  private val timeout = Opts
    .option[Int](
      "timeout",
      "Some failed inserts cannot be written to BigQuery, even after multiple attempts. " +
        "These events are buffered and ultimately sunk to GCS." +
        "The buffer is flushed when bufferSize is reached or when timeout passes."
    )
    .validate("Timeout must be greater than 0.") { _ > 0 }
    .withDefault(DefaultTimeout)

  private val backoffPeriod = Opts
    .option[Int]("backoffPeriod", "Time (in seconds) to wait before re-trying to insert the record(s).")
    .validate("Backoff period must be greater than 0.") { _ > 0 }
    .withDefault(DefaultBackoffTime)

  private val verbose: Opts[Boolean] = Opts.flag("verbose", "Provide debug output").orFalse

  case class ListenCommand(
    env: RepeaterEnvironment,
    bufferSize: Int,
    timeout: Int,
    backoffPeriod: Int,
    verbose: Boolean
  )

  val command: Command[ListenCommand] = Command(generated.BuildInfo.name, generated.BuildInfo.description) {
    (options, bufferSize, timeout, backoffPeriod, verbose).mapN(ListenCommand.apply)
  }

  def validateBucket(s: String): ValidatedNel[String, GcsPath] =
    if (s.startsWith(GcsPrefix)) {
      s.drop(GcsPrefix.length).split("/").toList match {
        case h :: _ if h.isEmpty => "GCS bucket cannot be empty".invalidNel
        case bucket :: path =>
          val p = path.mkString("/")
          GcsPath(bucket, if (p.endsWith("/")) p else p ++ "/").validNel
        case Nil => "GCS bucket cannot be empty".invalidNel
      }
    } else {
      s"GCS bucket must start with $GcsPrefix".invalidNel
    }

  def parse(args: Seq[String]): Either[Help, ListenCommand] = command.parse(args)
}
