package com.snowplowanalytics.snowplow.storage.bqmutator
package generator

import org.specs2.Specification

class SuggestionSpec extends Specification { def is = s2"""
  stringSuggestion produces nullable field even with required $e1
  stringSuggestion produces nothing for union type $e2
  """

  def e1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": ["null", "string"]}
      """.stripMargin)

    val expected = BigQueryField.Primitive(BigQueryType.String, FieldMode.Nullable)

    Suggestion.stringSuggestion(input, true) must beSome(expected)
  }

  def e2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": ["integer", "string"]}
      """.stripMargin)

    Suggestion.stringSuggestion(input, true) must beNone
  }
}
