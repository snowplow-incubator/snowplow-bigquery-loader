package com.snowplowanalytics.snowplow.storage.bigquery.common

import cats.data.{EitherT, NonEmptyList}
import org.specs2.mutable.Specification
import cats.{Id, Monad}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.iglu.schemaddl.bigquery.Mode.Nullable
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.Record
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.String
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type.Integer
import com.snowplowanalytics.lrumap.CreateLruMap
import com.snowplowanalytics.snowplow.storage.bigquery.common.LoaderRow.getSchemaAsField
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers.iglu._
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers.clocks.stoppedTimeClock

import scala.concurrent.duration._

class CachedFieldSpec extends Specification {
  val fieldForOriginalSchema = Field("", Record(List(Field("field1", String, Nullable))), Nullable)
  val fieldForPatchedSchema =
    Field("", Record(List(Field("field1", String, Nullable), Field("field2", Integer, Nullable))), Nullable)

  val schemaKey = SchemaKey
    .fromUri("iglu:com.snowplowanalytics.snowplow/test_schema/jsonschema/1-0-0")
    .getOrElse(throw new Exception("invalid schema key in cached field spec"))

  private def getCache: FieldCache[Id] = CreateLruMap[Id, FieldKey, Field].create(100)

  private def getOriginalResolver: Resolver[Id] = resolver(staticRegistry(cachedSchemas(`original schema - 1 field`)))

  private def getPatchedResolver(originalResolver: Resolver[Id]): Resolver[Id] = {
    val newRepos = List(staticRegistry(cachedSchemas(`patched schema - 2 fields`)))
    originalResolver.copy(repos = newRepos)
  }

  private def getFieldWithTime(resolver: Resolver[Id], fieldCache: FieldCache[Id], t: Int) =
    getSchemaAsField(resolver, schemaKey, fieldCache)(Monad[Id], RegistryLookup[Id], stoppedTimeClock(t.toLong))

  private def getResult(in: EitherT[Id, NonEmptyList[LoaderIgluError], Field]) =
    in.value.getOrElse(throw new Exception("we expected a field to be created but something went wrong"))

  "Cached fields should be in sync with cached schemas/lists in iglu client" >> {

    "(1) original schema only, 1 field cached" in {
      val fieldCache = getCache
      val resolver   = getOriginalResolver

      val result = getResult(getFieldWithTime(resolver, fieldCache, 1000))

      result must beEqualTo(fieldForOriginalSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome
    }

    "(2) original schema is patched between calls, no delay => original schema is still cached => 1 field in cache" in {
      val fieldCache       = getCache
      val originalResolver = getOriginalResolver
      val patchedResolver  = getPatchedResolver(originalResolver)

      //first call
      getFieldWithTime(originalResolver, fieldCache, 1000)

      //second call, same time
      val result = getResult(getFieldWithTime(patchedResolver, fieldCache, 1000))

      //no data from patched schema
      result must beEqualTo(fieldForOriginalSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome
    }

    "(3) schema is patched, delay between getSchemaAsField calls is less than cache TTL => original schema is still cached => 1 field cached" in {
      val fieldCache       = getCache
      val originalResolver = getOriginalResolver
      val patchedResolver  = getPatchedResolver(originalResolver)

      //first call
      getFieldWithTime(originalResolver, fieldCache, 1000)

      //second call, 2s later, less than 10s TTL
      val result = getResult(getFieldWithTime(patchedResolver, fieldCache, 3000))

      //no data from patched schema
      result must beEqualTo(fieldForOriginalSchema)

      //Field cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Field is not cached after second call (3 seconds)
      fieldCache.get((schemaKey, 3.second)) must beNone
    }

    "(4) schema is patched, delay between getSchemaAsField calls is greater than cache TTL => original schema is expired => using patched schema => 2 fields cached" in {
      val fieldCache       = getCache
      val originalResolver = getOriginalResolver
      val patchedResolver  = getPatchedResolver(originalResolver)

      //first call
      getFieldWithTime(originalResolver, fieldCache, 1000)

      //second call, 12s later - greater than 10s TTL
      val result = getResult(getFieldWithTime(patchedResolver, fieldCache, 13000))

      //Cache content expired, patched schema is fetched => expected field is based on the patched schema
      result must beEqualTo(fieldForPatchedSchema)

      //Field is cached after first call (1 second)
      fieldCache.get((schemaKey, 1.second)) must beSome

      //Field is cached after second call (13 seconds)
      fieldCache.get((schemaKey, 13.second)) must beSome
    }
  }
}
