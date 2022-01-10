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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.time.Instant
import java.util.UUID

import io.circe.syntax._
import io.circe.parser._

import org.specs2.mutable.Specification

class EventContainerSpec extends Specification {
  "EventContainer" should {
    "have isomorphic codecs" in {
      val eventId = UUID.randomUUID()
      val etlTstamp = Instant.now()
      val testJson =
        parse(s"""
          | {
          |   "event_id": "$eventId",
          |   "etl_tstamp": "$etlTstamp",
          |   "example_field1": "example_val1",
          |   "example_field2": "example_val2"
          | }
          |""".stripMargin)
      val originalEventContainer = EventContainer(eventId, etlTstamp, testJson.toOption.get.asObject.get)
      val resultEventContainer = originalEventContainer.asJson.as[EventContainer].toOption.get
      originalEventContainer mustEqual(resultEventContainer)
    }
  }
}
