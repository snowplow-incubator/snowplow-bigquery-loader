package com.snowplowanalytics.snowplow.storage.bigquery.common

import cats.Applicative
import org.specs2.mutable.Specification
import cats.effect.Clock
import cats.{Id, Monad}
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryError.NotFound
import com.snowplowanalytics.iglu.client.resolver.registries.{RegistryError, Registry, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList}
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.UnstructEvent
import io.circe.Json
import io.circe.literal.JsonStringContext

import scala.concurrent.duration._
import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS}
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

  val cacheTtl = 10.seconds
  val schemaKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/test_schema/jsonschema/1-0-0").getOrElse(throw new Exception("invalid schema key in cached field spec"))
  val shreddedType = ShreddedType(UnstructEvent, schemaKey)
  val columnName = Schema.getColumnName(shreddedType)

  "Cached fields should be in sync with cached schemas/lists in iglu client" >> {

    "(1) original schema only, 1 field cached" in {
      val propertiesCache = getCache
      val result = getSchemaAsField(propertiesCache, getResolver)(
        currentTime = 1000, //ms
        schemaInRegistry = `original schema - 1 field`
      )

      result must beEqualTo(fieldFor(`original schema - 1 field`))

      //Properties are cached after first call (1 second)
      propertiesCache.get((schemaKey, 1.second)) must beSome
    }

    "(2) original schema is patched between calls, no delay => original schema is still cached => 1 field in cache" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(fieldCache, getResolver)(
        currentTime = 1000, //ms
        schemaInRegistry = `original schema - 1 field`
      )

      //second call, same time
      val result = getSchemaAsField(fieldCache, resolver)(
        currentTime = 1000, //ms
        schemaInRegistry = `patched schema - 2 fields` //different schema with the same key!
      )

      //no data from patched schema
      result must beEqualTo(fieldFor(`original schema - 1 field`))

      //Properties are cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome
    }

    "(3) schema is patched, delay between getSchemaAsField calls is less than cache TTL => original schema is still cached => 1 field cached" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(fieldCache, resolver)(
        currentTime = 1000, //ms
        schemaInRegistry = `original schema - 1 field`
      )

      //second call, 2s later, less than 10s TTL
      val result = getSchemaAsField(fieldCache, resolver)(
        currentTime = 3000, //ms
        schemaInRegistry = `patched schema - 2 fields` //different schema with the same key!
      )

      //no data from patched schema
      result must beEqualTo(fieldFor(`original schema - 1 field`))

      //Properties are cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Properties are not cached after second call (3 seconds)
      fieldCache.get((schemaKey, 3.second)) must beNone
    }

    "(4) schema is patched, delay between flatten calls is greater than cache TTL => original schema is expired => using patched schema => 2 fields cached" in {
      val fieldCache = getCache
      val resolver = getResolver

      //first call
      getSchemaAsField(fieldCache, resolver)(
        currentTime = 1000, //ms
        schemaInRegistry = `original schema - 1 field`
      )

      //second call, 12s later - greater than 10s TTL
      val result = getSchemaAsField(fieldCache, resolver)(
        currentTime = 13000, //ms
        schemaInRegistry = `patched schema - 2 fields` //different schema with the same key!
      )

      //Cache content expired, patched schema is fetched => expected field is based on the patched schema
      result must beEqualTo(fieldFor(`patched schema - 2 fields`))

      //Properties are cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Properties are cached after second call (13 seconds)
      fieldCache.get((schemaKey, 13.second)) must beSome
    }
  }

  private def fieldFor(json: Json) = DdlSchema.parse(json).map(schema => Field.build("", schema, false)).get
  //Helper method to wire all test dependencies and execute LoaderRow.getSchemaAsField
  private def getSchemaAsField(fieldCache: FieldCache[Id], resolver: Resolver[Id])(schemaInRegistry: Json, currentTime: Long): Field = {

    //To return value stored in the schemaInRegistry variable, passed registry is ignored
    val testRegistryLookup: RegistryLookup[Id] = new RegistryLookup[Id] {
      override def lookup(registry: Registry, schemaKey: SchemaKey): Id[Either[RegistryError, Json]] = Right(schemaInRegistry)

      override def list(registry: Registry,
                        vendor: String,
                        name: String,
                        model: Int): Id[Either[RegistryError, SchemaList]] = Left(NotFound) // not used
    }

    val idClock: Clock[Id] = new Clock[Id] {
      def realTime: FiniteDuration =
        FiniteDuration(currentTime, MILLISECONDS)

      def monotonic: FiniteDuration =
        FiniteDuration(currentTime * 1000000, NANOSECONDS)

      def applicative: Applicative[Id] = implicitly[Applicative[Id]]
    }

    LoaderRow.getSchemaAsField(resolver, schemaKey, fieldCache)(Monad[Id], testRegistryLookup, idClock)
      .value
      .getOrElse(throw new Exception("something went wrong")) //todo what is the done thing now rightprojection is deprecated

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
