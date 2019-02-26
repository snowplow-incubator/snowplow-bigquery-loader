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
package mutator

import cats.implicits._
import cats.effect.{ IO, Clock }
import com.monovore.decline._

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._

import common.Codecs
import common.Config._

/** Mutator-specific CLI configuration */
object CommandLine {

  private implicit val privateIoClock: Clock[IO] =
    Clock.create[IO]

  private val options = (resolverOpt, configOpt)
    .mapN { (resolver, config) => EnvironmentConfig(resolver, config) }

  val schema: Opts[SchemaKey] = Opts.option[String]("schema", "Iglu URI to add to the table").mapValidated { schema =>
    SchemaKey.fromUri(schema) match {
      case Right(schemaKey) => schemaKey.validNel
      case Left(error) => s"$schema is not a valid Iglu URI, ${error.code}".invalidNel
    }
  }

  val property: Opts[ShredProperty] = Opts.option[String]("shred-property", s"Snowplow shred property (${Codecs.ValidProperties})").mapValidated {
    prop => Codecs.decodeShredProperty(prop).toValidatedNel
  }

  val verbose = Opts.flag("verbose", "Provide debug output").orFalse

  sealed trait MutatorCommand extends Product with Serializable {
    def config: EnvironmentConfig
    def getEnv: IO[Environment] = transform[IO](config).value.flatMap(IO.fromEither[Environment])
  }
  case class CreateCommand(config: EnvironmentConfig) extends MutatorCommand
  case class ListenCommand(config: EnvironmentConfig, verbose: Boolean) extends MutatorCommand
  case class AddColumnCommand(config: EnvironmentConfig, schema: SchemaKey, property: ShredProperty) extends MutatorCommand

  val createCmd = Opts.subcommand("create", "Create empty table and exit")(options.map(CreateCommand.apply))
  val listenCmd = Opts.subcommand("listen", "Run mutator and listen for new types")((options, verbose).mapN(ListenCommand.apply))
  val addColumn = Opts.subcommand("add-column", "Add column to the BigQuery table")((options, schema, property).mapN(AddColumnCommand.apply))

  val command = Command(generated.BuildInfo.name, generated.BuildInfo.description)(createCmd.orElse(listenCmd).orElse(addColumn))

  def parse(args: Seq[String]): Either[Help, MutatorCommand] = command.parse(args)
}
