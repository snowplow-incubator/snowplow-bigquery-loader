/*
 * Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.Base64
import java.nio.file.{Files, Path}

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

import _root_.io.circe.Json
import _root_.io.circe.parser.{parse => jsonParse}

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

import com.monovore.decline.Argument

sealed trait EncodedOrPath[+A]

object EncodedOrPath {

  final case class FromBase64[A](unresolved: A) extends EncodedOrPath[A]
  final case class PathToContent(path: Path) extends EncodedOrPath[Nothing]

  private val base64 = Base64.getDecoder

  trait ParseOps[A] {
    def errorMessage: String
    def parseString(from: String): Either[String, A]
  }

  object ParseOps {
    def apply[A](implicit ev: ParseOps[A]): ParseOps[A] = ev

    implicit def encodedHoconOrPathOps: ParseOps[Config] =
      new ParseOps[Config] {
        def errorMessage = "Could not parse argument as a base64-encoded HOCON"

        def parseString(from: String): Either[String, Config] =
          Either.catchOnly[ConfigException](ConfigFactory.parseString(from)).leftMap(_.getMessage)
      }

    implicit def encodedJsonOrPathOps: ParseOps[Json] =
      new ParseOps[Json] {
        def errorMessage = "Could not parse argument as a base64-encoded JSON"

        def parseString(from: String): Either[String, Json] =
          jsonParse(from).leftMap(_.show)
      }

  }

  implicit def encodedOrPathArgument[A: ParseOps]: Argument[EncodedOrPath[A]] =
    new Argument[EncodedOrPath[A]] {
      def read(string: String): ValidatedNel[String, EncodedOrPath[A]] =
        tryEncoded[A](string) match {
          case Right(encoded) => encoded.valid
          case Left(encodedError) =>
            val pathArg = Argument[Path].read(string).andThen { path =>
              if (Files.exists(path))
                PathToContent(path).valid
              else
                NonEmptyList.one("Could not pass argument as a file: File does not exist").invalid
            }

            pathArg match {
              case Validated.Valid(file)     => file.valid
              case Validated.Invalid(errors) => errors.prepend(encodedError).invalid
            }
        }

      def defaultMetavar: String = "path or base64-encoded"
    }

  def tryEncoded[A: ParseOps](str: String): Either[String, FromBase64[A]] = {
    val result = for {
      bytes  <- Either.catchOnly[IllegalArgumentException](base64.decode(str)).leftMap(_.getMessage)
      config <- ParseOps[A].parseString(new String(bytes))
    } yield FromBase64(config)

    result.leftMap(e => s"${ParseOps[A].errorMessage}: $e")
  }

}
