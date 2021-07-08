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
package com.snowplowanalytics.snowplow.storage.bigquery.common.config

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model._

import cats.{Id, Monad}
import cats.data.{EitherT, ValidatedNel}
import cats.implicits.toShow
import cats.syntax.either._
import com.monovore.decline.Opts
import com.typesafe.config.ConfigFactory
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.config.parser
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.parse

import java.util.Base64

final case class CliConfig(
  projectId: String,
  loader: Config.Loader,
  mutator: Config.Mutator,
  repeater: Config.Repeater,
  monitoring: Monitoring
)

object CliConfig {
  final case class Environment[A](config: A, resolverJson: Json, projectId: String, monitoring: Monitoring) {
    def getFullSubName(sub: String): String     = s"projects/$projectId/subscriptions/$sub"
    def getFullTopicName(topic: String): String = s"projects/$projectId/topics/$topic"
  }

  object Environment {
    type LoaderEnvironment   = Environment[Config.Loader]
    type MutatorEnvironment  = Environment[Config.Mutator]
    type RepeaterEnvironment = Environment[Config.Repeater]
  }

  implicit val cliConfigDecoder: Decoder[CliConfig] = deriveDecoder[CliConfig]
  implicit val cliConfigEncoder: Encoder[CliConfig] = deriveEncoder[CliConfig]

  /** CLI option to parse base64-encoded resolver into JSON */
  val resolverOpt: Opts[Json] = Opts
    .option[String]("resolver", "Base64-encoded Iglu Resolver configuration (self-describing json)")
    .mapValidated(validate(decodeBase64Json))

  /** CLI option to parse base64-encoded config hocon */
  val configOpt: Opts[CliConfig] =
    Opts.option[String]("config", "Base64-encoded app configuration (hocon)").mapValidated(validate(decodeBase64Hocon))

  /** Check if the provided resolverJson can be parsed into a Resolver instance */
  def validateResolverJson[F[_]: Monad: InitSchemaCache: InitListCache](
    resolverJson: Json
  ): EitherT[F, Throwable, Resolver[F]] =
    EitherT[F, DecodingFailure, Resolver[F]](Resolver.parse[F](resolverJson)).leftMap(df =>
      new RuntimeException(df.show)
    )

  def decodeBase64Json(base64Str: String): Either[Throwable, Json] =
    for {
      text <- Either.catchNonFatal(new String(Base64.getDecoder.decode(base64Str)))
      json <- parse(text)
      _    <- validateResolverJson[Id](json).value
    } yield json

  def decodeBase64Hocon(base64Str: String): Either[Throwable, CliConfig] =
    for {
      text  <- Either.catchNonFatal(new String(Base64.getDecoder.decode(base64Str)))
      hocon <- parseHocon(text)
    } yield hocon

  private def parseHocon(str: String): Either[Throwable, CliConfig] =
    for {
      resolved <- Either.catchNonFatal(ConfigFactory.parseString(str).resolve)
      conf     <- Either.catchNonFatal(ConfigFactory.load(resolved.withFallback(ConfigFactory.load())))
      cliConf  <- parser.decode[CliConfig](conf)
    } yield cliConf

  private def validate[In, Out](f: In => Either[Throwable, Out])(a: In): ValidatedNel[String, Out] =
    f(a).leftMap(_.getMessage).toValidatedNel
}
