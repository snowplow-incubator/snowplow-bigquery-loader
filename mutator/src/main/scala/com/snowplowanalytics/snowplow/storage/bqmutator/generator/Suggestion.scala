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
package com.snowplowanalytics.snowplow.storage.bqmutator.generator

import org.json4s.JsonAST._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.{ CommonProperties, StringProperties}

object Suggestion {

  val stringSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.String) =>
        Some(BigQueryField.Primitive(BigQueryType.String, FieldMode.get(required)))
      case Some(CommonProperties.Product(types)) if withNull(types, CommonProperties.String) =>
        Some(BigQueryField.Primitive(BigQueryType.String, FieldMode.Nullable))
      case _ => None
    }

  val booleanSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Boolean) =>
        Some(BigQueryField.Primitive(BigQueryType.Boolean, FieldMode.get(required)))
      case Some(CommonProperties.Product(types)) if withNull(types, CommonProperties.Boolean) =>
        Some(BigQueryField.Primitive(BigQueryType.Boolean, FieldMode.Nullable))
      case _ => None
    }

  val integerSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Integer) =>
        Some(BigQueryField.Primitive(BigQueryType.Integer, FieldMode.get(required)))
      case Some(CommonProperties.Product(types)) if withNull(types, CommonProperties.Integer) =>
        Some(BigQueryField.Primitive(BigQueryType.Integer, FieldMode.Nullable))
      case _ => None
    }

  val floatSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Number) =>
        Some(BigQueryField.Primitive(BigQueryType.Float, FieldMode.get(required)))
      case Some(CommonProperties.Product(types)) if onlyNumeric(types.toSet, true) =>
        Some(BigQueryField.Primitive(BigQueryType.Float, FieldMode.Nullable))
      case Some(CommonProperties.Product(types)) if onlyNumeric(types.toSet, false)  =>
        Some(BigQueryField.Primitive(BigQueryType.Float, FieldMode.get(required)))
      case Some(CommonProperties.Product(types)) if withNull(types, CommonProperties.Number) =>
        Some(BigQueryField.Primitive(BigQueryType.Float, FieldMode.Nullable))
      case _ => None
    }

  val complexEnumSuggestion: Suggestion = (schema, required) =>
    schema.enum match {
      case Some(CommonProperties.Enum(values)) =>
        Some(fromEnum(values, required))
      case _ => None
    }

  val datetimeSuggestion: Suggestion = (schema, required) =>
    (schema.`type`, schema.format) match {
      case (Some(CommonProperties.String), Some(StringProperties.DateFormat)) =>
        Some(BigQueryField.Primitive(BigQueryType.Date, FieldMode.get(required)))
      case (Some(CommonProperties.Product(types)), Some(StringProperties.DateFormat)) if withNull(types, CommonProperties.String) =>
        Some(BigQueryField.Primitive(BigQueryType.Date, FieldMode.Nullable))

      case (Some(CommonProperties.String), Some(StringProperties.DateTimeFormat)) =>
        Some(BigQueryField.Primitive(BigQueryType.DateTime, FieldMode.get(required)))
      case (Some(CommonProperties.Product(types)), Some(StringProperties.DateTimeFormat)) if withNull(types, CommonProperties.String) =>
        Some(BigQueryField.Primitive(BigQueryType.DateTime, FieldMode.Nullable))

      case _ => None
    }

  def finalSuggestion(required: Boolean) =
    BigQueryField.Primitive(BigQueryType.String, FieldMode.get(required))

  val suggestions: List[Suggestion] = List(
    datetimeSuggestion,
    booleanSuggestion,
    stringSuggestion,
    integerSuggestion,
    complexEnumSuggestion
  )

  private[generator] def fromEnum(enums: List[JValue], required: Boolean): BigQueryField = {
    def isString(json: JValue) = json.isInstanceOf[JString] || json == JNull
    def isInteger(json: JValue) = json.isInstanceOf[JInt] || json == JNull
    def isNumeric(json: JValue) =
      json.isInstanceOf[JInt] || json.isInstanceOf[JDouble] || json.isInstanceOf[JDecimal] || json == JNull
    val noNull: Boolean = !enums.contains(JNull)

    if (enums.forall(isString)) {
      BigQueryField.Primitive(BigQueryType.String, FieldMode.get(required && noNull))
    } else if (enums.forall(isInteger)) {
      BigQueryField.Primitive(BigQueryType.Integer, FieldMode.get(required && noNull))
    } else if (enums.forall(isNumeric)) {
      BigQueryField.Primitive(BigQueryType.Float, FieldMode.get(required && noNull))
    } else {
      BigQueryField.Primitive(BigQueryType.String, FieldMode.get(required && noNull))
    }
  }

  private def withNull(types: List[CommonProperties.Type], t: CommonProperties.Type): Boolean =
    types.toSet == Set(t, CommonProperties.Null) || types == List(t)

  private def onlyNumeric(types: Set[CommonProperties.Type], allowNull: Boolean): Boolean =
    if (allowNull) types == Set(CommonProperties.Number, CommonProperties.Integer, CommonProperties.Null)
    else types == Set(CommonProperties.Number, CommonProperties.Integer)
}
