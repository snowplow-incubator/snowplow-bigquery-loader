/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery
package repeater

import cats.implicits._
import com.monovore.decline._

import common.Config._

/** Repeater-specific CLI configuration (actually copy of Mutator's) */
object RepeaterCli {    // TODO: factor out into a common module
  private val GcsPrefix = "gs://"

  val DefaultWindow = 30
  val DefaultBufferSize = 20
  val DefaultBackoffTime = 900
  val DefaultMaxConcurrentInsertion = 5
  val DefaultMaxAckDeadlinePeriod = 10

  case class GcsPath(bucket: String, path: String)

  private val options = (resolverOpt, configOpt)
    .mapN { (resolver, config) => EnvironmentConfig(resolver, config) }

  val failedInsertsSub = Opts.option[String]("failedInsertsSub", "Provide debug output")
  val deadEndBucket = Opts.option[String]("deadEndBucket", "GCS path to sink failed events")
    .mapValidated { s =>
      if (s.startsWith(GcsPrefix)) { s.drop(GcsPrefix.length).split("/").toList match {
        case bucket :: path =>
          val p = path.mkString("/")
          GcsPath(bucket, if (p.endsWith("/")) p else p ++ "/").validNel
        case Nil => "GCS bucket cannot be empty".invalidNel
      } } else { s"GCS bucket must start with $GcsPrefix".invalidNel }
    }
  val bufferSize = Opts.option[Int]("desperatesBufferSize", "Amount of items that failed re-insertion that " +
    "will be buffered before sinking them to GCS, complementary to desperatesWindow")
    .validate("Buffer size needs to be greater than 0") { _ > 0 }
    .withDefault(DefaultBufferSize)
  val window = Opts.option[Int]("desperatesWindow", "Amount of seconds to wait until dump desperates to GCS. " +
    "Complementary to desperatesBufferSize")
    .validate("Time needs to be greater than 0") { _ > 0 }
    .withDefault(DefaultWindow)
  val backoffPeriod = Opts.option[Int]("backoffPeriod", "Amount of seconds to wait until re-insertion attempt will be made.")
    .validate("Time needs to be greater than 0") { _ > 0 }
    .withDefault(DefaultBackoffTime)
  val maxConcurrentInsertion = Opts.option[Int]("maxConcurrentInsertion", "Maximum number of concurrency during insertion")
    .validate("Maximum number of concurrency needs to be greater than 0") { _ > 0 }
    .withDefault(DefaultMaxConcurrentInsertion)
  val maxAckDeadlinePeriod = Opts.option[Int]("maxAckDeadlinePeriod", "Maximum PubSub ack message deadline period in second. " +
    "Events will not be resent during this period even ack is lost")
    .validate("Maximum ack deadline period needs to be greater than 10 second") { _ > 10 }
    .withDefault(DefaultMaxAckDeadlinePeriod)

  val verbose = Opts.flag("verbose", "Provide debug output").orFalse

  case class ListenCommand(config: EnvironmentConfig,
                           failedInsertsSub: String,
                           deadEndBucket: GcsPath,
                           verbose: Boolean,
                           bufferSize: Int,
                           window: Int,
                           backoff: Int,
                           maxConcurrentInsertion: Int,
                           maxAckDeadlinePeriod: Int)

  val command = Command(generated.BuildInfo.name, generated.BuildInfo.description) {
    (options, failedInsertsSub, deadEndBucket, verbose, bufferSize, window, backoffPeriod, maxConcurrentInsertion, maxAckDeadlinePeriod).mapN(ListenCommand.apply)
  }

  def parse(args: Seq[String]) = command.parse(args)

}
