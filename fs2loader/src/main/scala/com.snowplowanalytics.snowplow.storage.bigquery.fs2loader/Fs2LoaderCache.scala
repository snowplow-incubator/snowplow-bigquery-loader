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
package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader

import org.joda.time.Instant

import scala.collection.concurrent.TrieMap
import cats.data.EitherNel

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.snowplow.badrows.FailureDetails

/** Singleton used to cache the results of looking up a SchemaKey and transforming the
  * resulting JSON into a [[Field]].
  */
object Fs2LoaderCache {
  type Key      = SchemaKey
  type Value    = EitherNel[FailureDetails.LoaderIgluError, Field]
  type Deadline = Long

  val Ttl = 5 * 60 * 1000 // 5 mins

  private[fs2loader] val underlying = TrieMap.empty[Key, (Value, Deadline)]
  private def now(): Long           = Instant.now.getMillis

  private[fs2loader] def get(key: Key): Option[(Value, Deadline)] = underlying.get(key)

  // Overrides existing value for the same key. Sets ttl to 5 mins.
  private[fs2loader] def put(key: Key, value: Value, ttl: Long = Ttl): Unit = {
    val deadline: Deadline       = now() + ttl
    val tuple: (Value, Deadline) = (value, deadline)
    underlying += (key -> tuple)
  }

  def getOrLookup(key: Key)(f: => Value): Value =
    get(key) match {
      case Some((value, deadline)) if deadline > now() => value
      case _ =>
        val valueF = f
        put(key, valueF)
        valueF
    }
}
