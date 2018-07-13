package com.snowplowanalytics.snowplow.storage.bqloader.mutator.generator

import org.specs2.Specification
import BigQueryField._
import com.snowplowanalytics.snowplow.storage.bqloader.mutator.SpecHelpers

class SuggestionSpec extends Specification { def is = s2"""
  stringSuggestion produces nullable field even with required $e1
  stringSuggestion produces nothing for union type $e2
  """

  def e1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": ["null", "string"]}
      """.stripMargin)

    val expected = BigQueryField("foo", BigQueryType.String, FieldMode.Nullable)

    Suggestion.stringSuggestion(input, true).map(_.apply("foo")) must beSome(expected)
  }

  def e2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": ["integer", "string"]}
      """.stripMargin)

    Suggestion.stringSuggestion(input, true) must beNone
  }
}
