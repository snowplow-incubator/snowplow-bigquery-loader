/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.snowplowanalytics.iglu.client.Resolver

import cats.Id
import io.circe.Json

object singleton {

  /** Singleton for Resolver to maintain one per node */
  object ResolverSingleton {
    @volatile private var instance: Resolver[Id] = _

    /** Retrieve or build an instance of a Resolver */
    def get(r: Json): Resolver[Id] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = Resolver.parse[Id](r).fold(e => throw new RuntimeException(e.toString), identity)
          }
        }
      }
      instance
    }
  }
}
