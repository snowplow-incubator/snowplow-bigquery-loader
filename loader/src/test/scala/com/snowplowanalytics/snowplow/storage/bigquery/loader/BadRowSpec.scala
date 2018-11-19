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

import io.circe.Json

import cats.data.NonEmptyList

import org.specs2.Specification

object BadRowSpec extends Specification { def is = s2"""
  encoder produces correct JSON $e1
  decoder extracts correct BadRow $e2
  """

  def e1 = {
    val input = BadRow("foo", NonEmptyList.of("one", "two", "three"))
    val expected = Json.fromFields(List(
      ("line", Json.fromString("Zm9v")),
      ("errors", Json.fromValues(List(Json.fromString("one"), Json.fromString("two"), Json.fromString("three"))))
    ))
    val result = BadRow.encoder(input)
    result mustEqual expected
  }

  def e2 = {
    val input = Json.fromFields(List(
      ("line", Json.fromString("Zm9v")),
      ("errors", Json.fromValues(List(Json.fromString("one"), Json.fromString("two"), Json.fromString("three"))))
    ))
    val expected = BadRow("foo", NonEmptyList.of("one", "two", "three"))
    val result = BadRow.decoder.decodeJson(input)
    result must beRight(expected)
  }
}
