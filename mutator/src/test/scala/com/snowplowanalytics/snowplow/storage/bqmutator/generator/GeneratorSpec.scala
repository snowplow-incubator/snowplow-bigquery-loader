package com.snowplowanalytics.snowplow.storage.bqmutator
package generator

import scala.collection.immutable.SortedMap

import org.specs2.Specification

class GeneratorSpec extends Specification { def is = s2"""
  traverseSuggestions generated field for object with string and object $e1
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

    val expected = BigQueryField.Record(
      FieldMode.Required,
      SortedMap("objectKey" -> BigQueryField.Record(
        FieldMode.Nullable,
        SortedMap(
          "nestedKey1" -> BigQueryField.Primitive(BigQueryType.String,FieldMode.Nullable),
          "nestedKey2" -> BigQueryField.Primitive(BigQueryType.Integer,FieldMode.Nullable),
          "nestedKey3" -> BigQueryField.Primitive(BigQueryType.Boolean,FieldMode.Required))
      ),
        "stringKey" -> BigQueryField.Primitive(BigQueryType.String,FieldMode.Nullable)
      )
    )

    Generator.traverseSuggestions(input, true) must beEqualTo(expected)
  }
}
