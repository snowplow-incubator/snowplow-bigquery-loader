/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
  val verbose = Opts.flag("verbose", "Provide debug output").orFalse

  case class ListenCommand(config: EnvironmentConfig, failedInsertsSub: String, deadEndBucket: GcsPath, verbose: Boolean)

  val command = Command(generated.BuildInfo.name, generated.BuildInfo.description)((options, failedInsertsSub, deadEndBucket, verbose).mapN(ListenCommand.apply))

  def parse(args: Seq[String]) = command.parse(args)

}
