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

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field, Type => DataType}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.snowplow.storage.bigquery.common.{Codecs, LoaderRow}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig._
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.MutatorEnvironment

import com.google.cloud.bigquery.TimePartitioning

import cats.implicits._
import cats.data.NonEmptyList
import com.monovore.decline._

/** Mutator-specific CLI configuration */
object MutatorCli {
  private val options: Opts[MutatorEnvironment] = (configOpt, resolverOpt).mapN { (config, resolver) =>
    Environment(config.mutator, resolver, config.projectId, config.monitoring)
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

  private val partitionField: Opts[Field] = Opts.option[String]("partitionField", "Field that table partitioned by").mapValidated {
    fieldName =>
      val fields = Atomic.table.appended(LoaderRow.LoadTstampField)
      fields.find(_.name == fieldName).toValidNel[String]("Field does not exist")
        .ensure(NonEmptyList.one("Field's type isn't timestamp"))(_.fieldType == DataType.Timestamp)
  }

  // Currently, only "day" type is supported by the used version of Google Cloud SDK however in the newer versions
  // other types such as "hour", "month" and "year" are also supported. Therefore, this option is added already.
  private val partitioningType: Opts[TimePartitioning.Type] = Opts.option[String]("partitioningType", "The type of time partitioning")
    .withDefault("day")
    .mapValidated {
      partitioningType => partitioningType.toLowerCase match {
        case "day" => TimePartitioning.Type.DAY.validNel
        case _ => "Partitioning type needs to be one of the followings: [DAY]".invalidNel
      }
    }

  private val requirePartitionFilter: Opts[Boolean] = Opts.flag("requirePartitionFilter", "Make a partition filter required for queries").orFalse

  sealed trait MutatorCommand extends Product with Serializable
  object MutatorCommand {
    final case class Create(env: MutatorEnvironment,
                            partitionField: Field,
                            partitioningType: TimePartitioning.Type,
                            requirePartitionFilter: Boolean) extends MutatorCommand
    final case class Listen(env: MutatorEnvironment, verbose: Boolean) extends MutatorCommand
    final case class AddColumn(env: MutatorEnvironment, schema: SchemaKey, property: ShredProperty)
        extends MutatorCommand
  }

  private val createCmd: Opts[MutatorCommand.Create] =
    Opts.subcommand("create", "Create empty table and exit")(
      (options, partitionField, partitioningType, requirePartitionFilter).mapN(MutatorCommand.Create.apply)
    )
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
