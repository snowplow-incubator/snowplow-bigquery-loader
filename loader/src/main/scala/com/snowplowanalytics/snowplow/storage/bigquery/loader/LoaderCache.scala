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

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import scala.collection.mutable.{Map => MMap}

/** Singleton used to cache the results of looking up a SchemaKey and transforming the
  * resulting JSON into a [[Field]].
  */
object LoaderCache {
  type Key = SchemaKey
  type Value = Either[NonEmptyList[FailureDetails.LoaderIgluError], Field]

  private var underlying = MMap[Key, Value]().empty

  def get(key: Key): Option[Value] = underlying.get(key)
  def put(key: Key, value: Value): Unit = underlying += (key -> value)
  def getOrLookup(key: Key)(f: => Value): Value =
    get(key) match {
      case Some(v) => v
      case None =>
        put(key, f)
        f
    }
}
