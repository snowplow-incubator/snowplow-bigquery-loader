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
package com.snowplowanalytics.snowplow.storage.bqloader.mutator

import cats.implicits._
import cats.effect.IO

import com.monovore.decline._

import com.snowplowanalytics.snowplow.storage.bqloader.core.Config._

/** Mutator-specific CLI configuration */
object CommandLine {

  private val options = (resolverOpt, configOpt)
    .mapN { (resolver, config) => EnvironmentConfig(resolver, config) }

  sealed trait MutatorCommand extends Product with Serializable {
    def env: EnvironmentConfig
    def getEnv: IO[Environment] =
      IO.fromEither(transform(env))
  }
  case class CreateCommand(env: EnvironmentConfig) extends MutatorCommand
  case class ListenCommand(env: EnvironmentConfig) extends MutatorCommand

  val createCmd = Opts.subcommand("create", "Create empty table and exit")(options.map(CreateCommand.apply))
  val listenCmd = Opts.subcommand("listen", "Run mutator and listen for new types")(options.map(ListenCommand.apply))

  val command = Command("mutator", "Snowplow BigQuery Mutator")(createCmd.orElse(listenCmd))

  def parse(args: Seq[String]) = command.parse(args)
}
