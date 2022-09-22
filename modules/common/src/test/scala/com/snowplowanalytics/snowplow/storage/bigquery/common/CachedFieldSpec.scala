package com.snowplowanalytics.snowplow.storage.bigquery.common

import org.specs2.mutable.Specification
import cats.{Id, Monad}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryError.NotFound
import com.snowplowanalytics.iglu.client.resolver.registries.{RegistryError, Registry, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList}
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.iglu.schemaddl.bigquery.Mode.Nullable
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.Record
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.String
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.Integer
import com.snowplowanalytics.lrumap.CreateLruMap
import io.circe.Json
import io.circe.literal.JsonStringContext

import scala.concurrent.duration._
class CachedFieldSpec extends Specification {
  //single 'field1' field
  val `original schema - 1 field`: Json =
    json"""
         {
             "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
             "description": "Test schema 1",
             "self": {
                 "vendor": "com.snowplowanalytics.snowplow",
                 "name": "test_schema",
                 "format": "jsonschema",
                 "version": "1-0-0"
             },

             "type": "object",
             "properties": {
               "field1": { "type": "string"}
              }
         }
        """

  //same key as schema1, but with additional `field2` field.
  val `patched schema - 2 fields`: Json =
    json"""
         {
             "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
             "description": "Test schema 1",
             "self": {
                 "vendor": "com.snowplowanalytics.snowplow",
                 "name": "test_schema",
                 "format": "jsonschema",
                 "version": "1-0-0"
             },

             "type": "object",
             "properties": {
               "field1": { "type": "string" },
               "field2": { "type": "integer" }
              }
         }
        """

  val fieldForOriginalSchema = Field("", Record(List(Field("field1", String, Nullable))), Nullable)
  val fieldForPatchedSchema = Field("", Record(List(Field("field1", String, Nullable), Field("field2", Integer, Nullable))), Nullable)

  val schemaKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/test_schema/jsonschema/1-0-0").getOrElse(throw new Exception("invalid schema key in cached field spec"))

  "Cached fields should be in sync with cached schemas/lists in iglu client" >> {

    "(1) original schema only, 1 field cached" in {
      val fieldCache = getCache
      val resolver = getResolver

      val result = getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `original schema - 1 field`, timeInMs = 1000)

      result must beEqualTo(fieldForOriginalSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome
    }

    "(2) original schema is patched between calls, no delay => original schema is still cached => 1 field in cache" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `original schema - 1 field`, timeInMs = 1000)

      //second call, same time
      val result = getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `patched schema - 2 fields`, timeInMs = 1000)

      //no data from patched schema
      result must beEqualTo(fieldForOriginalSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome
    }

    "(3) schema is patched, delay between getSchemaAsField calls is less than cache TTL => original schema is still cached => 1 field cached" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `original schema - 1 field`, timeInMs = 1000)

      //second call, 2s later, less than 10s TTL
      val result = getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `patched schema - 2 fields`, timeInMs = 3000)

      //no data from patched schema
      result must beEqualTo(fieldForOriginalSchema)

      //Field cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Field is not cached after second call (3 seconds)
      fieldCache.get((schemaKey, 3.second)) must beNone
    }

    "(4) schema is patched, delay between getSchemaAsField calls is greater than cache TTL => original schema is expired => using patched schema => 2 fields cached" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `original schema - 1 field`, timeInMs = 1000)

      //second call, 12s later - greater than 10s TTL
      val result = getSchemaAsField(resolver, fieldCache)(schemaInRegistry = `patched schema - 2 fields`, timeInMs = 13000)

      //Cache content expired, patched schema is fetched => expected field is based on the patched schema
      result must beEqualTo(fieldForPatchedSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Field is cached after second call (13 seconds)
      fieldCache.get((schemaKey, 13.second)) must beSome
    }
  }

  //Helper method to wire all test dependencies and execute LoaderRow.getSchemaAsField
  private def getSchemaAsField(resolver: Resolver[Id], fieldCache: FieldCache[Id])(schemaInRegistry: Json, timeInMs: Long) = {

    //To return value stored in the schemaInRegistry variable, passed registry is ignored
    val testRegistryLookup: RegistryLookup[Id] = new RegistryLookup[Id] {
      override def lookup(registry: Registry, schemaKey: SchemaKey): Id[Either[RegistryError, Json]] = Right(schemaInRegistry)

      override def list(registry: Registry,
                        vendor: String,
                        name: String,
                        model: Int): Id[Either[RegistryError, SchemaList]] = Left(NotFound) // not used
    }

    LoaderRow.getSchemaAsField[Id](resolver, schemaKey, fieldCache)(Monad[Id], testRegistryLookup, SpecHelpers.clocks.stoppedTimeClock(timeInMs))
      .value
      .getOrElse(throw new Exception("we expected a field to be created but something went wrong"))
  }

  private def getCache: FieldCache[Id] = CreateLruMap[Id, FieldKey, Field].create(100)

  private def getResolver: Resolver[Id] = {
    Resolver.init[Id](
      cacheSize = 10,
      cacheTtl = Some(10.seconds),
      refs = Registry.Embedded( //not used in test as we fix returned schema in custom test RegistryLookup
        Registry.Config("Test", 0, List.empty),
        path = "/fake"
      )
    )
  }
}
