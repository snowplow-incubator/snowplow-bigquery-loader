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
package com.snowplowanalytics.snowplow.storage.bigquery.common.config

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model._

import cats.Id
import cats.implicits._

import com.typesafe.config.{Config => TypesafeConfig, ConfigException, ConfigFactory}
import io.circe.{Decoder, Encoder, Json}
import io.circe.config.parser
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.parse

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final case class AllAppsConfig(
  projectId: String,
  loader: Config.Loader,
  mutator: Config.Mutator,
  repeater: Config.Repeater,
  monitoring: Monitoring
)

object AllAppsConfig {

  implicit val allAppsConfigDecoder: Decoder[AllAppsConfig] = deriveDecoder[AllAppsConfig]
  implicit val allAppsConfigEncoder: Encoder[AllAppsConfig] = deriveEncoder[AllAppsConfig]

  def fromRaw(raw: CliConfig): Either[String, (Json, AllAppsConfig)] =
    for {
      igluJson <- readJson(raw.resolver)
      hocon    <- readHocon(raw.config)
      allApps  <- parseHocon(hocon)
      _        <- Resolver.parse[Id](igluJson).leftMap(e => show"Cannot parse Iglu resolver: $e")
    } yield (igluJson, allApps)

  def readJson(in: EncodedJsonOrPath): Either[String, Json] =
    in match {
      case EncodedJsonOrPath.Base64Json(json) =>
        json.asRight
      case EncodedJsonOrPath.PathToJson(path) =>
        for {
          text <- textFromFile(path)
          json <- (parse(text)).leftMap(e => s"Cannot parse JSON in ${path.toAbsolutePath}: ${e.getMessage}")
        } yield json
    }

  private def readHocon(in: Option[EncodedHoconOrPath]): Either[String, TypesafeConfig] =
    in match {
      case Some(EncodedHoconOrPath.Base64Hocon(unresolved)) =>
        Either
          .catchOnly[ConfigException](unresolved.resolve)
          .leftMap(e => s"Cannot resolve Base64 HOCON: ${e.getMessage}")
      case Some(EncodedHoconOrPath.PathToHocon(path)) =>
        for {
          text <- textFromFile(path)
          hocon <- Either
            .catchOnly[ConfigException](ConfigFactory.parseString(text))
            .leftMap(e => s"Cannot parse HOCON in ${path.toAbsolutePath}: ${e.getMessage}")
          hocon <- Either
            .catchOnly[ConfigException](hocon.resolve)
            .leftMap(e => s"Cannot resolve HOCON in ${path.toAbsolutePath}: ${e.getMessage}")
        } yield hocon
      case None =>
        ConfigFactory.empty.asRight
    }

  private def textFromFile(path: Path): Either[String, String] =
    Either
      .catchNonFatal(Files.readAllLines(path).asScala.mkString("\n"))
      .leftMap(e => s"Error reading ${path.toAbsolutePath} file from filesystem: ${e.getMessage}")

  /** Uses the typesafe config layering approach. Loads configurations in the following priority order:
    *  1. System properties
    *  2. The provided configuration file
    *  3. application.conf of our app
    *  4. reference.conf of any libraries we use
    */
  private def parseHocon(in: TypesafeConfig): Either[String, AllAppsConfig] = {
    val all = namespaced(ConfigFactory.load(namespaced(in.withFallback(namespaced(ConfigFactory.load())))))
    parser.decode[AllAppsConfig](all).leftMap(_.show)
  }

  /** Optionally give precedence to configs wrapped in a "snowplow" block. To help avoid polluting config namespace */
  private def namespaced(config: TypesafeConfig): TypesafeConfig =
    if (config.hasPath(Namespace))
      config.getConfig(Namespace).withFallback(config.withoutPath(Namespace))
    else
      config

  private val Namespace = "snowplow"

}
