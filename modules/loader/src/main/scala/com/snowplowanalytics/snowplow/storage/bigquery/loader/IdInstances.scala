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

import cats.{Applicative, Id}
import cats.effect.Clock

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, NANOSECONDS}

object IdInstances {
  implicit val idClock: Clock[Id] = new Clock[Id] {
    def realTime: FiniteDuration =
      FiniteDuration(System.currentTimeMillis, MILLISECONDS)

    def monotonic: FiniteDuration =
      FiniteDuration(System.nanoTime(), NANOSECONDS)

    def applicative: Applicative[Id] = implicitly[Applicative[Id]]
  }
}
