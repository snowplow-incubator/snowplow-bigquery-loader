/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.permutive.pubsub.consumer.ConsumerRecord
import cats.effect.std.Queue
import fs2.Stream

package object repeater {
  type EventRecord[F[_]] = ConsumerRecord[F, EventContainer]

  type EventStream[F[_]] = Stream[F, EventRecord[F]]

  type EventQueue[F[_]] = Queue[F, EventRecord[F]]
}
