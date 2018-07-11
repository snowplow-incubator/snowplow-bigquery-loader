package com.snowplowanalytics.snowplow.storage.bqmutator

import org.json4s.jackson.parseJson

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.Json4sToSchema._

object SpecHelpers {
  def parseSchema(string: String): Schema = {
    Schema
      .parse(parseJson(string))
      .getOrElse(throw new RuntimeException("SpecHelpers.parseSchema received invalid JSON Schema"))
  }
}
