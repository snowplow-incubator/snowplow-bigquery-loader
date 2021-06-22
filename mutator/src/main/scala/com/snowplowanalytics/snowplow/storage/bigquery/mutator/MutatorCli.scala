/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import cats.implicits._
import com.monovore.decline._

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig._
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.MutatorEnvironment

/** Mutator-specific CLI configuration */
object MutatorCli {
  private val options: Opts[MutatorEnvironment] = (configOpt, resolverOpt).mapN { (config, resolver) =>
    Environment(config.mutator, resolver, config.projectId)
  }

  private val schema: Opts[SchemaKey] = Opts.option[String]("schema", "Iglu URI to add to the table").mapValidated {
    schema =>
      SchemaKey.fromUri(schema) match {
        case Right(schemaKey) => schemaKey.validNel
        case Left(error)      => s"$schema is not a valid Iglu URI, ${error.code}".invalidNel
      }
  }

  private val property: Opts[ShredProperty] =
    Opts.option[String]("shred-property", s"Snowplow shred property (${Codecs.ValidProperties})").mapValidated { prop =>
      Codecs.decodeShredProperty(prop).toValidatedNel
    }

  private val verbose: Opts[Boolean] = Opts.flag("verbose", "Provide debug output").orFalse

  sealed trait MutatorCommand extends Product with Serializable
  object MutatorCommand {
    final case class Create(env: MutatorEnvironment) extends MutatorCommand
    final case class Listen(env: MutatorEnvironment, verbose: Boolean) extends MutatorCommand
    final case class AddColumn(env: MutatorEnvironment, schema: SchemaKey, property: ShredProperty)
        extends MutatorCommand
  }

  private val createCmd: Opts[MutatorCommand.Create] =
    Opts.subcommand("create", "Create empty table and exit")(options.map(MutatorCommand.Create.apply))
  private val listenCmd: Opts[MutatorCommand.Listen] =
    Opts.subcommand("listen", "Run mutator and listen for new types")(
      (options, verbose).mapN(MutatorCommand.Listen.apply)
    )
  private val addColumn: Opts[MutatorCommand.AddColumn] =
    Opts.subcommand("add-column", "Add column to the BigQuery table")(
      (options, schema, property).mapN(MutatorCommand.AddColumn.apply)
    )

  private val command: Command[MutatorCommand] =
    Command(generated.BuildInfo.name, generated.BuildInfo.description)(createCmd.orElse(listenCmd).orElse(addColumn))

  def parse(args: Seq[String]): Either[Help, MutatorCommand] = command.parse(args)
}
