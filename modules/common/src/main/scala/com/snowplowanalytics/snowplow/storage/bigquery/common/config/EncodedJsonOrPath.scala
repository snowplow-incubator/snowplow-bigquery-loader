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
import _root_.io.circe.parser.parse

import com.monovore.decline.Argument

sealed trait EncodedJsonOrPath

object EncodedJsonOrPath {

  final case class Base64Json(json: Json) extends EncodedJsonOrPath
  final case class PathToJson(path: Path) extends EncodedJsonOrPath

  private val base64 = Base64.getDecoder

  implicit val encodedOrPathArgument: Argument[EncodedJsonOrPath] =
    new Argument[EncodedJsonOrPath] {
      def read(string: String): ValidatedNel[String, EncodedJsonOrPath] =
        tryEncoded(string) match {
          case Right(encoded) => encoded.valid
          case Left(encodedError) =>
            Argument[Path].read(string).andThen { path =>
              if (Files.exists(path))
                PathToJson(path).valid
              else
                NonEmptyList.one("Could not pass argument as a file: File does not exist").invalid
            } match {
              case Validated.Valid(file)     => file.valid
              case Validated.Invalid(errors) => errors.prepend(encodedError).invalid
            }
        }

      def defaultMetavar: String = "path or base64-encoded"
    }

  def tryEncoded(string: String): Either[String, Base64Json] = {
    val result = for {
      bytes <- Either.catchOnly[IllegalArgumentException](base64.decode(string)).leftMap(_.getMessage)
      str = new String(bytes)
      json <- parse(str).leftMap(_.show)
    } yield Base64Json(json)

    result.leftMap(e => s"Could not parse argument as base64-encoded JSON: $e")
  }

}
