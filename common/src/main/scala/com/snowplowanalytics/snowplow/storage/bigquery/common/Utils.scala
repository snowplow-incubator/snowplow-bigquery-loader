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

import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.fromJsonNode

import io.circe.Json

import cats.data.{Validated, ValidatedNel}

import com.fasterxml.jackson.databind.JsonNode

object Utils {
  @deprecated
  private def toCirce(json: JValue): Json =
    json match {
      case JString(string) => Json.fromString(string)
      case JInt(int) => Json.fromBigInt(int)
      case JBool(bool) => Json.fromBoolean(bool)
      case JArray(arr) => Json.fromValues(arr.map(toCirce))
      case JDouble(num) => Json.fromDoubleOrNull(num)
      case JDecimal(decimal) => Json.fromBigDecimal(decimal)
      case JObject(fields) => Json.fromFields(fields.map { case (k, v) => (k, toCirce(v)) })
      case _ => Json.Null
    }

  @deprecated
  private def fromJackson(json: JsonNode): Json =
    toCirce(fromJsonNode(json))

  @deprecated
  private def catchNonFatalMessage[A](a: => A): ValidatedNel[String, A] =
    Validated
      .catchNonFatal(a)
      .leftMap(_.getMessage)
      .toValidatedNel
}
