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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import io.circe.syntax._
import io.circe.Json
import cats.data.NonEmptyList
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import PayloadParser.{SelfDescribingEntity, ReconstructedEvent}
import BadRow.ParsingError
import org.specs2.Specification

object PayloadParserSpec {
  val flattenSchemaKeyContexts = "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
  val flattenSchemaKeyRandomPrefix = "random_contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
  val flattenSchemaKeyUnstructEvent = "unstruct_event_com_snowplowanalytics_snowplow_web_page_1_0_0"
}

class PayloadParserSpec extends Specification { def is= s2"""
  minimal payload without any context or unstruct event can be parsed successfully $e1
  parse payload with context array which has flatten schema key prefixed with "contexts" $e2
  parse payload with context array which has flatten schema key prefixed randomly $e3
  parse payload with more than one context array which has flatten schema key prefixed with "contexts" $e4
  parse payload with unstruct event which has flatten schema key prefixed with "unstruct_event" $e5
  parse payload with unstruct event and context array $e6
  parse payload which one of required event field is missing $e7
  """
  import PayloadParserSpec._

  def e1 = {
    val event = SpecHelpers.exampleMinimalEvent
    val expected = Right(ReconstructedEvent(event, List()))
    PayloadParser.parse(event.asJsonObject) mustEqual expected
  }

  def e2 = {
    val event = SpecHelpers.exampleMinimalEvent
    val (res, expected) = parseEventWithContext(
      event,
      contextFlattenSchemaKeys = List(flattenSchemaKeyContexts)
    )
    res mustEqual expected
  }


  def e3 = {
    val event = SpecHelpers.exampleMinimalEvent
    val (res, expected) = parseEventWithContext(
      event,
      contextFlattenSchemaKeys = List(flattenSchemaKeyRandomPrefix)
    )
    res mustNotEqual expected
  }

  def e4 = {
    val event = SpecHelpers.exampleMinimalEvent
    val (res, expected) = parseEventWithContext(
      event,
      contextFlattenSchemaKeys = List(
        s"${flattenSchemaKeyContexts}_1",
        s"${flattenSchemaKeyContexts}_2"
      )
    )
    res mustEqual expected
  }

  def e5 = {
    val event = SpecHelpers.exampleMinimalEvent
    val (res, expected) = parseEventWithContext(
      event,
      unstructEventFlattenSchemaKeys = List(flattenSchemaKeyUnstructEvent)
    )
    res mustEqual expected
  }

  def e6 = {
    val event = SpecHelpers.exampleMinimalEvent
    val (res, expected) = parseEventWithContext(
      event,
      contextFlattenSchemaKeys = List(flattenSchemaKeyContexts),
      unstructEventFlattenSchemaKeys = List(flattenSchemaKeyUnstructEvent)
    )
    res mustEqual expected
  }

  def e7 = {
    val eventJson = SpecHelpers.requiredFieldMissingEventJson
    val eventJsonStr = eventJson.noSpaces
    val res = PayloadParser.parse(eventJson.asObject.get) match {
      case Left(ParsingError(`eventJsonStr`, _, List("DownField(v_etl)"))) => true
      case _ => false
    }
    res must beTrue
  }

  private def parseEventWithContext(event: Event, contextFlattenSchemaKeys: List[String] = List(), unstructEventFlattenSchemaKeys: List[String] = List()) = {
    val contexts = createJsonAndSelfDescribingEntityPair(SpecHelpers.createContexts, contextFlattenSchemaKeys)
    val unstructEvents = createJsonAndSelfDescribingEntityPair(SpecHelpers.createUnstructEvent, unstructEventFlattenSchemaKeys)
    val unifiedEventJsonObject = (contexts ++ unstructEvents)
      .map(_._1)
      .foldLeft(event.asJsonObject.toMap - "unstruct_event" - "contexts" - "derived_contexts") { (c, i) =>
        c ++ i.asObject.get.toMap
      }.asJsonObject
    val jsonsWithFlattenSchemaKey = (contexts ++ unstructEvents).map(_._2)
    val expected = Right(
      ReconstructedEvent(
        event,
        jsonsWithFlattenSchemaKey
      )
    )
    val res = PayloadParser.parse(unifiedEventJsonObject)
    (res, expected)
  }

  private def createJsonAndSelfDescribingEntityPair(f: String => Json, flattenSchemaKeys: List[String]): List[(Json, SelfDescribingEntity)] =
    flattenSchemaKeys.map { key =>
      val json = f(key)
      val selfDescribingEntity = SelfDescribingEntity(
        key,
        json
          .hcursor
          .downField(key)
          .as[Json]
          .getOrElse(throw new RuntimeException("Value with schema key not found"))
      )
      (json, selfDescribingEntity)
    }
}


