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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import java.util.{ Base64, UUID }

import cats.data.{ ValidatedNel, EitherT }
import cats.syntax.show._
import cats.syntax.either._
import cats.effect.{Clock, IO}

import io.circe.{ Json, Decoder, DecodingFailure }
import io.circe.parser.parse
import io.circe.generic.semiauto._

import com.monovore.decline.Opts

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.validator.{ CirceValidator => Validator }

/**
  * Main storage target configuration file
  *
  * @param name Human-readable target name
  * @param id Unique target id
  * @param input PubSub topic with TSV enriched events
  * @param projectId Google Cloud project id
  * @param datasetId Google BigQuery dataset id
  * @param tableId Google BigQuery table id
  * @param load BigQuery loading API
  * @param typesTopic PubSub topic where Loader should **publish** new types
  * @param typesSubscription PubSub subscription (associated with `typesTopic`),
  *                 where Mutator pull types from
  */
case class Config(name: String,
                  id: UUID,
                  input: String,
                  projectId: String,
                  datasetId: String,
                  tableId: String,
                  load: Config.LoadMode,
                  typesTopic: String,
                  typesSubscription: String,
                  badRows: String,
                  failedInserts: String) {
  def getFullInput: String = s"projects/$projectId/subscriptions/$input"
  def getFullTypesTopic: String = s"projects/$projectId/topics/$typesTopic"

  def getFullBadRowsTopic: String = s"projects/$projectId/topics/$badRows"
  def getFullFailedInsertsTopic: String = s"projects/$projectId/topics/$failedInserts"
}

object Config {

  /** An exception that can be raised during initialization on e.g. invalid configuration */
  case class InitializationError(message: String) extends Throwable {
    override def getMessage: String = message
  }

  /** Common pure configuration for Loader and Mutator */
  case class EnvironmentConfig(resolver: Json, config: Json)

  /** Parsed common environment (resolver is a stateful object) */
  class Environment private[Config](val config: Config,
                                    val resolverJson: Json) extends Serializable

  sealed trait LoadMode
  object LoadMode {
    case class StreamingInserts(retry: Boolean) extends LoadMode
    case class FileLoads(frequency: Int) extends LoadMode

    implicit val loadModeCirceDecoder: Decoder[LoadMode] =
      Decoder.instance { cursor =>
        cursor.downField("mode").as[String] match {
          case Right("STREAMING_INSERTS") =>
            cursor.downField("retry").as[Boolean].map(b => StreamingInserts(b))
          case Right("FILE_LOADS") =>
            cursor.downField("frequence").as[Int].map(f => FileLoads(f))
          case Right(unkown) =>
            DecodingFailure(s"Unknown mode [$unkown]", cursor.history).asLeft
          case Left(error) =>
            error.asLeft
        }
      }
  }

  implicit val configCirceDecoder: Decoder[Config] = deriveDecoder[Config]

  private implicit val clock: Clock[IO] = Clock.create[IO]

  def transform(config: EnvironmentConfig): EitherT[IO, InitializationError, Environment] =
    for {
      resolver   <- EitherT[IO, DecodingFailure, Resolver[IO]](Resolver.parse(config.resolver)).leftMap(err => InitializationError(err.show))
      jsonConfig <- EitherT.fromEither[IO](SelfDescribingData.parse(config.config)).leftMap(err => InitializationError(s"Configuration is not self-describing, ${err.code}"))
      client      = Client(resolver, Validator)
      _          <- client.check(jsonConfig).leftMap(err => InitializationError(s"Validation failure: $err"))
      result     <- EitherT.fromEither[IO](jsonConfig.data.as[Config]).leftMap(err => InitializationError(s"Decoding failure: ${err.show}"))

    } yield new Environment(result, config.resolver)

  /** CLI option to parse base64-encoded resolver into JSON */
  val resolverOpt: Opts[Json] = Opts.option[String]("resolver", "Base64-encoded Iglu Resolver configuration")
    .mapValidated(toValidated(decodeBase64Json))

  /** CLI option to parse base64-encoded config into JSON */
  val configOpt: Opts[Json] = Opts.option[String]("config", "Base64-encoded BigQuery configuration")
    .mapValidated(toValidated(decodeBase64Json))

  def decodeBase64Json(base64: String): Either[Throwable, Json] =
    for {
      text <- Either.catchNonFatal(new String(Base64.getDecoder.decode(base64)))
      json <- parse(text)
    } yield json

  private def toValidated[A, R](f: A => Either[Throwable, R])(a: A): ValidatedNel[String, R] =
    f(a).leftMap(_.getMessage).toValidatedNel
}
