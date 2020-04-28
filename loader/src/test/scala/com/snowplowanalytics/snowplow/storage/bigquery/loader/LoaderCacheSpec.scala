/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import java.time.Instant
import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.LookupHistory
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field, Mode, Type}
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import org.specs2.mutable.Specification

class LoaderCacheSpec extends Specification {
  import com.snowplowanalytics.snowplow.storage.bigquery.loader.LoaderCache._

  sequential

  "get should" >> {
    "return None if the key is not in the cache" >> {
      val __ = underlying.clear()
      val key = SchemaKey("com.acme", "missing_key", "jsonschema", SchemaVer.Full(1, 0, 0))

      get(key) shouldEqual None
    }

    "return the value if the key is in the cache" >> {
      val __ = underlying.clear()
      val key = SchemaKey("com.acme", "available_key", "jsonschema", SchemaVer.Full(1, 0, 0))
      val value = Right(Field("com_acme_available_key_1", Type.String, Mode.Nullable))
      val _ = put(key, value)
      val result = get(key)

      result.get._1 shouldEqual value
    }
  }

  "put should" >> {
    "add a value to the cache" >> {
      val __ = underlying.clear()
      val key = SchemaKey("com.acme", "new_key", "jsonschema", SchemaVer.Full(1, 0, 0))
      val value = Right(Field("com_acme_new_key_1", Type.String, Mode.Nullable))

      { put(key, value); get(key).get._1 } shouldEqual value
    }

    "override an existing key in the cache" >> {
      val __ = underlying.clear()
      val key = SchemaKey("com.acme", "existing_key", "jsonschema", SchemaVer.Full(1, 0, 0))
      val oldValue = Right(Field("com_acme_existing_key_1", Type.String, Mode.Nullable))
      val newValue = Right(Field("com_acme_existing_key_1", Type.String, Mode.Repeated))
      val _ = put(key, oldValue)
      val cacheSize = underlying.size

      get(key).get._1 shouldNotEqual { put(key, newValue); get(key).get._1 }
      { put(key, oldValue); put(key, newValue); underlying.size } shouldEqual cacheSize
    }
  }

  "getOrLookup should" >> {
    val key = SchemaKey("com.acme", "existing_key", "jsonschema", SchemaVer.Full(1, 0, 0))
    val value = Right(Field("com_acme_existing_key_1", Type.String, Mode.Nullable))
    val error = Left(NonEmptyList.one(FailureDetails.LoaderIgluError.IgluError(key, ClientError.ResolutionError(Map("1" -> LookupHistory(Set(), 1, Instant.now))))))

    "get the value from cache if it's there" >> {
      val __ = underlying.clear()
      val _ = put(key, value)

      getOrLookup(key)(error) shouldEqual value
    }

    "not add the result of f to cache if a value is already there" >> {
      val __ = underlying.clear()
      val _ = put(key, value)

      get(key).get._1 shouldEqual getOrLookup(key)(error)
    }

    "add the result of f to cache if no value is already there, and return it" >> {
      val __ = underlying.clear()

      { getOrLookup(key)(error); get(key).get._1 } shouldEqual error
    }
  }
}
