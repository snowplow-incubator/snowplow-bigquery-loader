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
package com.snowplowanalytics.snowplow.storage.bqloader.mutator
package generator

import org.specs2.Specification

import BigQueryField._


class GeneratorSpec extends Specification { def is = s2"""
  build generates field for object with string and object $e1
  build recognizes numeric properties $e2
  build generated repeated field for array $e3
  """

  def e1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)

    val expected = BigQueryField(
      "foo",
      BigQueryType.Record(List(
        BigQueryField("objectKey",
          BigQueryType.Record(List(
            BigQueryField("nestedKey3", BigQueryType.Boolean, FieldMode.Required),
            BigQueryField("nestedKey1", BigQueryType.String, FieldMode.Nullable),
            BigQueryField("nestedKey2", BigQueryType.Integer, FieldMode.Nullable)
          )),
          FieldMode.Nullable
        ),
        BigQueryField("stringKey", BigQueryType.String,FieldMode.Nullable))),
      FieldMode.Nullable
    )

    Generator.build("foo", input, false) must beEqualTo(expected)
  }

  def e2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "numeric1": {"type": "number" },
        |  "numeric2": {"type": "integer" },
        |  "numeric3": {"type": ["number", "null"] },
        |  "numeric4": {"type": ["integer", "null", "number"] }
        |},
        |"required": ["numeric4", "numeric2"]
        |}
      """.stripMargin)

    val expected = BigQueryField(
      "foo",
      BigQueryType.Record(List(
        BigQueryField("numeric2", BigQueryType.Integer, FieldMode.Required),
        BigQueryField("numeric1", BigQueryType.Float, FieldMode.Nullable),
        BigQueryField("numeric3", BigQueryType.Float, FieldMode.Nullable),
        BigQueryField("numeric4", BigQueryType.Float, FieldMode.Nullable)
      )),
      FieldMode.Nullable
    )

    Generator.build("foo", input, false) must beEqualTo(expected)
  }

  def e3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "array",
        |"items": {
        |  "type": "object",
        |  "properties": {
        |    "foo": { "type": "string" },
        |    "bar": { "type": "integer" }
        |  }
        |}
        |}
      """.stripMargin)

    val expected = BigQueryField(
      "foo",
      BigQueryType.Record(List(
        BigQueryField("bar", BigQueryType.Integer, FieldMode.Nullable),
        BigQueryField("foo", BigQueryType.String, FieldMode.Nullable)
      )),
      FieldMode.Repeated
    )

    Generator.build("foo", input, false) must beEqualTo(expected)
  }
}
