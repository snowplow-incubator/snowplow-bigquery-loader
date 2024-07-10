/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.data.NonEmptyVector
import org.specs2.Specification
import org.specs2.matcher.MatchResult
import io.circe.Json
import io.circe.literal._
import io.circe.parser.{parse => parseAsCirce}
import org.json.{JSONArray, JSONObject}

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.loaders.transform.TabledEntity

import java.util.UUID
import java.time.Instant
import scala.collection.immutable.SortedMap

class LegacyColumnsTransformSpec extends Specification {
  import LegacyColumnsTransformSpec._

  def is = s2"""
  LegacyColumns.transformEvent should
    Transform an event with only atomic fields (no custom entities) $onlyAtomic
    Transform an event with one custom context $oneContext
    Transform an event with an unstruct event $unstruct
    Transform an unstruct event with each different type $unstructAllTypes
    Transform a context with different type $contextAllTypes
    Produce JSON null on output for unstruct column if no data matching type is provided $unstructNoData
    Produce JSON null on output for contexts column if no data matching type is provided $contextsNoData

  Failures:
    Missing value for unstruct (missing required field) $unstructMissingValue
    Missing value for unstruct (null passed in required field) $unstructNullValue
    Missing value for context (missing required field) $contextMissingValue
    Missing value for context (null passed in required field) $contextNullValue
    Cast error for unstruct (integer passed in string field) $unstructWrongType
    Cast error for context (integer passed in string field) $contextWrongType
    Iglu error in batch info becomes iglu transformation error $igluErrorInBatchInfo
  """

  def onlyAtomic = {
    val event     = createEvent()
    val batchInfo = LegacyColumns.Result(Vector.empty, List.empty) // no custom entities

    assertSuccessful(event, batchInfo, expectedEntities = Map.empty)
  }

  def oneContext = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_int": 42}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "contexts_com_example_my_schema_1_0_0" -> json"""[{"my_int": 42}]"""
    )

    assertSuccessful(inputEvent, batchInfo, expectedEntities = expectedOutput)
  }

  def unstruct = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_int": 42}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "unstruct_event_com_example_my_schema_1_0_0" -> json"""{"my_int": 42}"""
    )

    assertSuccessful(inputEvent, batchInfo, expectedEntities = expectedOutput)
  }

  def unstructAllTypes = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = dataWithAllTypes, key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(SchemaVer.Full(1, 0, 0), schemaWithAllPossibleTypes)),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "unstruct_event_com_example_my_schema_1_0_0" -> json"""{
        "my_string":   "abc",
        "my_int":       42,
        "my_long":      42000000000,
        "my_decimal":   1.23,
        "my_double":    1.2323,
        "my_boolean":   true,
        "my_date":      19801,
        "my_timestamp": 1710879639000000,
        "my_array":     [1,2,3],
        "my_object":    {"abc": "xyz"},
        "my_empty_object": "{}"
      }"""
    )

    assertSuccessful(inputEvent, batchInfo, expectedEntities = expectedOutput)
  }

  def contextAllTypes = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = dataWithAllTypes, key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(SchemaVer.Full(1, 0, 0), schemaWithAllPossibleTypes)),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "contexts_com_example_my_schema_1_0_0" -> json"""[{
        "my_string":   "abc",
        "my_int":       42,
        "my_long":      42000000000,
        "my_decimal":   1.23,
        "my_double":    1.2323,
        "my_boolean":   true,
        "my_date":      19801,
        "my_timestamp": 1710879639000000,
        "my_array":     [1,2,3],
        "my_object":    {"abc": "xyz"},
        "my_empty_object": "{}"
      }]"""
    )

    assertSuccessful(inputEvent, batchInfo, expectedEntities = expectedOutput)
  }

  def unstructNoData = {
    val inputEvent = createEvent()

    val batchTypesInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "unstruct_event_com_example_my_schema_1_0_0" -> null
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedEntities = expectedOutput)
  }

  def contextsNoData = {
    val inputEvent = createEvent()

    val batchTypesInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )
    val expectedOutput = Map(
      "contexts_com_example_my_schema_1_0_0" -> null
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedEntities = expectedOutput)
  }

  def unstructMissingValue = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = json"""null""",
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def unstructNullValue = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_int": null}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = Json.Null,
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def contextMissingValue = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = json"""null""",
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def contextNullValue = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_int": null}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = Json.Null,
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def unstructWrongType = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_int": "xyz"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaUnstruct(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = json""""xyz"""",
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def contextWrongType = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_int": "xyz"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = LegacyColumns.Result(
      fields       = Vector(mySchemaContexts(version = SchemaVer.Full(1, 0, 0))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
      value    = json""""xyz"""",
      expected = "Integer"
    )

    assertLoaderError(inputEvent, batchInfo, List(expectedError))
  }

  def igluErrorInBatchInfo = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_int": 42}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))

    val igluResolutionError = FailureDetails.LoaderIgluError.SchemaListNotFound(
      SchemaCriterion("com.example", "mySchema", "jsonschema", 1),
      ResolutionError(SortedMap.empty)
    )

    val batchInfo = LegacyColumns.Result(
      fields = Vector.empty,
      igluFailures = List(
        LegacyColumns.ColumnFailure(
          SchemaKey("com.example", "mySchema", "jsonschema", SchemaVer.Full(1, 0, 0)),
          TabledEntity.UnstructEvent,
          igluResolutionError
        )
      )
    )

    assertLoaderError(inputEvent, batchInfo, expectedErrors = List(igluResolutionError))
  }

  private def assertSuccessful(
    event: Event,
    batchInfo: LegacyColumns.Result,
    expectedEntities: Map[String, Json]
  ) = {
    val result = LegacyColumns.transformEvent(BadRowProcessor("test-loader", "0.0.0"), event, batchInfo)

    result must beRight { actualValues: Map[String, AnyRef] =>
      val actualFieldNames = actualValues.keys
      val actualValuesAsCirce = actualValues.map {
        case (k, arr: JSONArray)  => k -> parseAsCirce(arr.toString).toOption.get
        case (k, obj: JSONObject) => k -> parseAsCirce(obj.toString).toOption.get
        case (k, other)           => k -> other
      }

      val assertEntitiesExist: MatchResult[Any] = actualValuesAsCirce must containAllOf(expectedEntities.toSeq)
      val totalNumberOfFields: MatchResult[Any] =
        actualFieldNames.size must beEqualTo(expectedEntities.size) // entities only

      assertEntitiesExist and totalNumberOfFields
    }
  }

  private def assertLoaderError(
    inputEvent: Event,
    batchInfo: LegacyColumns.Result,
    expectedErrors: List[FailureDetails.LoaderIgluError]
  ): MatchResult[Either[BadRow, Map[String, AnyRef]]] = {
    val result = LegacyColumns.transformEvent(BadRowProcessor("test-loader", "0.0.0"), inputEvent, batchInfo)

    result must beLeft.like { case BadRow.LoaderIgluError(_, Failure.LoaderIgluErrors(errors), _) =>
      errors.toList must containTheSameElementsAs(expectedErrors)
    }
  }

}

object LegacyColumnsTransformSpec {

  private val simpleOneFieldSchema =
    NonEmptyVector.of(
      Field("my_int", Type.Integer, Required)
    )

  private val schemaWithAllPossibleTypes =
    NonEmptyVector.of(
      Field("my_string", Type.Json, Required),
      Field("my_int", Type.Integer, Required),
      Field("my_long", Type.Long, Required),
      Field("my_decimal", Type.Decimal(Type.DecimalPrecision.Digits9, 2), Required),
      Field("my_double", Type.Double, Required),
      Field("my_boolean", Type.Boolean, Required),
      Field("my_date", Type.Date, Required),
      Field("my_timestamp", Type.Timestamp, Required),
      Field("my_array", Type.Array(Type.Integer, Required), Required),
      Field("my_object", Type.Struct(NonEmptyVector.of(Field("abc", Type.String, Required))), Required),
      Field("my_empty_object", Type.Json, Required),
      Field("my_null", Type.String, Nullable)
    )

  private val dataWithAllTypes = json"""
   {
     "my_string":   "abc",
     "my_int":       42,
     "my_long":      42000000000,
     "my_decimal":   1.23,
     "my_double":    1.2323,
     "my_boolean":   true,
     "my_date":      "2024-03-19",
     "my_timestamp": "2024-03-19T20:20:39Z",
     "my_array":     [1,2,3],
     "my_object":    {"abc": "xyz"},
     "my_null":      null,
     "my_empty_object": {}
   }
   """

  private val collectorTstamp = Instant.now.minusSeconds(42L)
  private val eventId         = UUID.randomUUID()

  private def createEvent(unstruct: Option[SelfDescribingData[Json]] = None, contexts: List[SelfDescribingData[Json]] = List.empty): Event =
    Event
      .minimal(eventId, collectorTstamp, "0.0.0", "0.0.0")
      .copy(unstruct_event = SnowplowEvent.UnstructEvent(unstruct))
      .copy(contexts = SnowplowEvent.Contexts(contexts))

  private def sdj(data: Json, key: String): SelfDescribingData[Json] =
    SelfDescribingData[Json](SchemaKey.fromUri(key).toOption.get, data)

  private def mySchemaContexts(
    version: SchemaVer.Full,
    ddl: NonEmptyVector[Field] = simpleOneFieldSchema
  ): LegacyColumns.FieldForEntity =
    LegacyColumns.FieldForEntity(
      field = Field(
        s"contexts_com_example_my_schema_${version.model}_${version.revision}_${version.addition}",
        Type.Array(Type.Struct(ddl), Nullable),
        Nullable
      ),
      key        = SchemaKey("com.example", "mySchema", "jsonschema", version),
      entityType = TabledEntity.Context
    )

  private def mySchemaUnstruct(
    version: SchemaVer.Full,
    ddl: NonEmptyVector[Field] = simpleOneFieldSchema
  ): LegacyColumns.FieldForEntity = LegacyColumns.FieldForEntity(
    field =
      Field(s"unstruct_event_com_example_my_schema_${version.model}_${version.revision}_${version.addition}", Type.Struct(ddl), Nullable),
    key        = SchemaKey("com.example", "mySchema", "jsonschema", version),
    entityType = TabledEntity.UnstructEvent
  )
}
