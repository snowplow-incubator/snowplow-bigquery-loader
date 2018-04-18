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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import java.util.Base64

import cats.implicits._
import cats.data.NonEmptyList

import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.syntax._

/**
  * Class representing an enriched event that failed BigQuery Loader transformation,
  * e.g. due unavailable schema, bug in Schema DDL or unexpected structure
  * Unlike "failed insert" it did not get to BigQuery table
  */
case class BadRow(line: String, errors: NonEmptyList[String]) {
  def compact: String = this.asJson.noSpaces
}

object BadRow {
  private val Line = "line"
  private val Errors = "errors"

  implicit val encoder: Encoder[BadRow] =
    Encoder.instance { badRow: BadRow =>
      val encoded = Base64.getEncoder.encode(badRow.line.getBytes)
      Json.fromFields(List(
        (Line, Json.fromString(new String(encoded))),
        (Errors, Json.fromValues(badRow.errors.toList.map(Json.fromString)))
      ))
    }

  implicit val decoder: Decoder[BadRow] =
    Decoder.instance[BadRow] { cursor =>
      for {
        jsonObject <- cursor.value
          .asObject
          .toRight(DecodingFailure("Bad row should be an object", cursor.history))
        map = jsonObject.toMap
        line <- map.get(Line)
          .flatMap(_.asString)
          .toRight(DecodingFailure(s"$Line should be a string", cursor.history))
        decodedLine <- Either.catchNonFatal(new String(Base64.getDecoder.decode(line)))
          .leftMap(error => DecodingFailure(s"Cannot decode $Line ${error.getMessage}", cursor.history))
        errors <- map.get(Errors)
          .flatMap(_.as[NonEmptyList[String]].toOption)
          .toRight(DecodingFailure(s"Cannot decode $Errors", cursor.history))

      } yield BadRow(decodedLine, errors)
    }
}
