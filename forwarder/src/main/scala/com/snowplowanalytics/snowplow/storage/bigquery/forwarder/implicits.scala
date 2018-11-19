/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.forwarder

import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.options.ValueProvider.{NestedValueProvider, StaticValueProvider}
import org.apache.beam.sdk.transforms.SerializableFunction

object implicits {
  def pure[A](a: A): ValueProvider[A] =
    StaticValueProvider.of(a)

  implicit class ValueProviderOps[A](val value: ValueProvider[A]) extends AnyVal {
    def map[B](fn: A => B): ValueProvider[B] = {
      val mapper: SerializableFunction[A, ValueProvider[B]] =
        new SerializableFunction[A, ValueProvider[B]] {
          def apply(input: A): ValueProvider[B] =
            pure(fn(input))
        }

      NestedValueProvider.of(value, mapper).get()
    }

    def flatten[B](implicit ev: A <:< ValueProvider[B]): ValueProvider[B] =
      if (value.isAccessible) {
        value.get()
      } else {
        map { v =>
          if (v.isAccessible) {
            v.get()
          } else {
            throw new RuntimeException(s"Cannot flatten inaccessible ValueProvider $value")
          }
        }
      }

    def ap[B, C](b: ValueProvider[B])(f: (A, B) => C): ValueProvider[C] =
      map { aa => b.map { bb => f(aa, bb) } }.flatten
  }
}
