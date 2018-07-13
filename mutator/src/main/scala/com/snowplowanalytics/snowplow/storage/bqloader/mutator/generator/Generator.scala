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
package com.snowplowanalytics.snowplow.storage.bqloader.mutator.generator

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.schemaddl.jsonschema.{ArrayProperties, CommonProperties, Schema}

import BigQueryField._

object Generator {

  /** Top-level declaration */
  case class Column(name: String, bigQueryField: BigQueryField, version: SchemaKey)

  def build(name: String, topSchema: Schema, required: Boolean): BigQueryField = {
    topSchema.`type` match {
      case Some(CommonProperties.Object) =>
        val subfields = topSchema.properties.map(_.value).getOrElse(Map.empty)
        if (subfields.isEmpty) {
          Suggestion.finalSuggestion(required)(name)
        } else {
          val requiredKeys = topSchema.required.toList.flatMap(_.value)
          val fields = subfields.map { case (key, schema) =>
            build(key, schema, requiredKeys.contains(key))
          }
          val subFields = fields.toList.sortBy(field => (FieldMode.sort(field.mode), field.name))
          BigQueryField(name, BigQueryType.Record(subFields), FieldMode.get(required))
        }
      case Some(CommonProperties.Array) =>
        topSchema.items match {
          case Some(ArrayProperties.ListItems(schema)) =>
            build(name, schema, false).copy(mode = FieldMode.Repeated)
          case _ =>
            Suggestion.finalSuggestion(required)(name)
        }
      case _ =>
        Suggestion.suggestions
          .find(suggestion => suggestion(topSchema, required).isDefined)
          .flatMap(_.apply(topSchema, required))
          .getOrElse(Suggestion.finalSuggestion(required))
          .apply(name)
    }
  }
}
