/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.IO
import cats.data.NonEmptyVector
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.loaders.transform.TabledEntity

class LegacyColumnsResolveSpec extends Specification with CatsEffect {
  import LegacyColumnsResolveSpec._

  def is = s2"""
  LegacyColumns.resolveTypes
    when resolving for known schemas in unstruct_event should
      return a single schema if the batch uses a single schema in a series $ue1
      return multiple schemas if the batch uses multiple schemas from a series $ue2
      return a JSON field for the Iglu Central ad_break_end_event schema $ue3
      return a JSON field for the Iglu Central anything-a schema $ue4
      return nothing if this is not a legacy schema $ue5

    when resolving for known schemas in contexts should
      return a single schema if the batch uses a single schema in a series $c1
      return multiple schemas if the batch uses multiple schemas from a series $c2
      return a JSON field for the Iglu Central ad_break_end_event schema $c3
      return a JSON field for the Iglu Central anything-a schema $c4
      return nothing if this is not a legacy schema $c5

    when resolving for known schema in contexts and unstruct_event should
      return separate entity for the context and the unstruct_event $both1

    when handling Iglu failures should
      return a IgluError if schema key does not exist in a valid series of schemas $fail1
      return an InvalidSchema if the series contains a schema that cannot be parsed $fail2
      return no failures if this is not a legacy schema $fail3
  """

  def ue1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake"))
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7_0_0", expectedStruct, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
        TabledEntity.UnstructEvent
      )
    }

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def ue2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (1, 0))
    )

    val expected100 = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake"))
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7_0_0", expectedStruct, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
        TabledEntity.UnstructEvent
      )
    }

    val expected110 = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake")),
          Field("col_other", Type.Long, Nullable, Set("col_other"))
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7_1_0", expectedStruct, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 1, 0)),
        TabledEntity.UnstructEvent
      )
    }

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(2)) and
        (fields must contain(expected100)) and
        (fields must contain(expected110))
    }

  }

  def ue3 = {

    // Example of a schema which is an empty object with no additional properties
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.media", "ad_break_end_event", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedType = Type.Json
      val expectedField =
        Field("unstruct_event_com_snowplowanalytics_snowplow_media_ad_break_end_event_1_0_0", expectedType, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
        TabledEntity.UnstructEvent
      )
    }

    val legacyCriteria = List(SchemaCriterion("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", 1))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def ue4 = {

    // Example of a permissive schema which permits any JSON
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.iglu", "anything-a", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedType  = Type.Json
      val expectedField = Field("unstruct_event_com_snowplowanalytics_iglu_anything_a_1_0_0", expectedType, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("com.snowplowanalytics.iglu", "anything-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        TabledEntity.UnstructEvent
      )
    }

    val legacyCriteria = List(SchemaCriterion("com.snowplowanalytics.iglu", "anything-a", "jsonschema", 1))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def ue5 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    // non-matching schema criteria:
    val legacyCriteria = List(SchemaCriterion("myvendor", "different", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must beEmpty)
    }

  }

  def c1 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake"))
        )
      )

      val expectedArray = Type.Array(expectedStruct, Nullable)

      val expectedField = Field("contexts_myvendor_myschema_7_0_0", expectedArray, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
        TabledEntity.Context
      )
    }

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def c2 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (1, 0))
    )

    val expected100 = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake"))
        )
      )

      val expectedArray = Type.Array(expectedStruct, Nullable)

      val expectedField = Field("contexts_myvendor_myschema_7_0_0", expectedArray, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
        TabledEntity.Context
      )
    }

    val expected110 = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_camel", Type.Json, Required, Set("colCamel")),
          Field("col_snake", Type.Json, Required, Set("col_snake")),
          Field("col_other", Type.Long, Nullable, Set("col_other"))
        )
      )

      val expectedArray = Type.Array(expectedStruct, Nullable)

      val expectedField = Field("contexts_myvendor_myschema_7_1_0", expectedArray, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 1, 0)),
        TabledEntity.Context
      )
    }

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(2)) and
        (fields must contain(expected100)) and
        (fields must contain(expected110))
    }

  }

  def c3 = {

    // Example of a schema which is an empty object with no additional properties
    val tabledEntity = TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_end_event", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedArray = Type.Array(Type.Json, Nullable)
      val expectedField =
        Field("contexts_com_snowplowanalytics_snowplow_media_ad_break_end_event_1_0_0", expectedArray, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
        TabledEntity.Context
      )
    }

    val legacyCriteria = List(SchemaCriterion("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", 1))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def c4 = {

    // Example of a permissive schema which permits any JSON
    val tabledEntity = TabledEntity(TabledEntity.Context, "com.snowplowanalytics.iglu", "anything-a", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {

      val expectedType = Type.Json

      val expectedArray = Type.Array(expectedType, Nullable)

      val expectedField = Field("contexts_com_snowplowanalytics_iglu_anything_a_1_0_0", expectedArray, Nullable)

      LegacyColumns.FieldForEntity(
        expectedField,
        SchemaKey("com.snowplowanalytics.iglu", "anything-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        TabledEntity.Context
      )
    }

    val legacyCriteria = List(SchemaCriterion("com.snowplowanalytics.iglu", "anything-a", "jsonschema", 1))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields must contain(expected))
    }

  }

  def c5 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    // non-matching schema criteria
    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 4))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must beEmpty)
    }

  }

  def both1 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)
    val tabledEntity2 = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity1 -> Set((0, 0)),
      tabledEntity2 -> Set((0, 0))
    )

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(2)) and
        (fields.map(_.entityType) must contain(allOf[TabledEntity.EntityType](TabledEntity.UnstructEvent, TabledEntity.Context)))
    }

  }

  def fail1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 9))
    )

    val expectedKey = SchemaKey("myvendor", "myschema", "jsonschema", SchemaVer.Full(7, 0, 9))

    val legacyCriteria = List(SchemaCriterion("myvendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must haveSize(1)) and
        (failures.head must beLike { case failure: LegacyColumns.ColumnFailure =>
          (failure.schemaKey must beEqualTo(expectedKey)) and
            (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.IgluError => ok })
        })
    }

  }

  def fail2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "invalid_syntax", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expectedKey = SchemaKey("myvendor", "invalid_syntax", "jsonschema", SchemaVer.Full(1, 0, 0))

    val legacyCriteria = List(SchemaCriterion("myvendor", "invalid_syntax", "jsonschema", 1))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must haveSize(1)) and
        (failures.head must beLike { case failure: LegacyColumns.ColumnFailure =>
          (failure.schemaKey must beEqualTo(expectedKey)) and
            (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.InvalidSchema => ok })
        })
    }

  }

  def fail3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 9))
    )

    // non-matching crieria
    val legacyCriteria = List(SchemaCriterion("other.vendor", "myschema", "jsonschema", 7))

    LegacyColumns.resolveTypes(embeddedResolver, input, legacyCriteria).map { case LegacyColumns.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must beEmpty)
    }

  }

}

object LegacyColumnsResolveSpec {

  // A resolver that resolves embedded schemas only
  val embeddedResolver = Resolver[IO](Nil, None)
}
