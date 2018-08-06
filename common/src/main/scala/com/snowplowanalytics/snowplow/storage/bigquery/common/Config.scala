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

import scalaz.NonEmptyList

import org.json4s._
import org.json4s.ext.JavaTypesSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods.parse

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline.Opts

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue.validateAndIdentifySchema

/**
  * Main storage target configuration file
  *
  * @param name Human-readable target name
  * @param id Unique target id
  * @param input PubSub topic with TSV enriched events
  * @param projectId Google Cloud project id
  * @param datasetId Google BigQuery dataset id
  * @param tableId Google BigQuery table id
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
                  typesTopic: String,
                  typesSubscription: String,
                  badOutput: String) {
  def getFullInput: String = s"projects/$projectId/subscriptions/$input"
  def getFullTypesTopic: String = s"projects/$projectId/topics/$typesTopic"
}

object Config {

  /** Common pure configuration for Loader and Mutator */
  case class EnvironmentConfig(resolver: JValue, config: JValue)

  /** Parsed common environment (resolver is a stateful object) */
  class Environment private[Config](val resolver: Resolver, val config: Config, val original: JValue) extends Serializable

  private implicit val formats: org.json4s.Formats =
    Serialization.formats(NoTypeHints) ++ JavaTypesSerializers.all

  /** Parse  */
  def transform(config: EnvironmentConfig): Either[Throwable, Environment] = {
    for {
      resolver <- Resolver.parse(config.resolver).fold(asThrowableLeft, _.asRight)
      _ = println(resolver)
      (_, data) <- validateAndIdentifySchema(config.config, dataOnly = true)(resolver).fold(asThrowableLeft, _.asRight)
      _ = println(data)
      result <- Either.catchNonFatal(data.extract[Config])
    } yield new Environment(resolver, result, config.resolver)
  }

  /** CLI option to parse base64-encoded resolver into JSON */
  val resolverOpt: Opts[JValue] = Opts.option[String]("resolver", "Base64-encoded Iglu Resolver configuration")
    .mapValidated(toValidated(decodeBase64Json))

  /** CLI option to parse base64-encoded config into JSON */
  val configOpt: Opts[JValue] = Opts.option[String]("config", "Base64-encoded BigQuery configuration")
    .mapValidated(toValidated(decodeBase64Json))

  def decodeBase64Json(base64: String): Either[Throwable, JValue] =
    for {
      text <- Either.catchNonFatal(new String(Base64.getDecoder.decode(base64)))
      json <- Either.catchNonFatal(parse(text))
    } yield json

  private def toValidated[A, R](f: A => Either[Throwable, R])(a: A): ValidatedNel[String, R] =
    f(a).leftMap(_.getMessage).toValidatedNel

  private def asThrowableLeft[A](errors: NonEmptyList[A]) = {
    println(errors)
    new RuntimeException(errors.list.mkString(", ")).asLeft
  }
}
