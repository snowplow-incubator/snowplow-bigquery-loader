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

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.storage.bigquery.common.SpecHelpers
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.PayloadParser.{ReconstructedEvent, SelfDescribingEntity}

import io.circe.Json
import io.circe.syntax._
import org.specs2.mutable.Specification

object PayloadParserSpec {
  val flattenSchemaKeyContexts      = "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
  val flattenSchemaKeyRandomPrefix  = "random_contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
  val flattenSchemaKeyUnstructEvent = "unstruct_event_com_snowplowanalytics_snowplow_web_page_1_0_0"
}

class PayloadParserSpec extends Specification {
  import PayloadParserSpec._

  private def parseEventWithContext(
    event: Event,
    contextFlattenSchemaKeys: List[String]       = List(),
    unstructEventFlattenSchemaKeys: List[String] = List()
  ) = {
    val contexts = createJsonAndSelfDescribingEntityPair(SpecHelpers.utils.createContexts, contextFlattenSchemaKeys)
    val unstructEvents =
      createJsonAndSelfDescribingEntityPair(SpecHelpers.utils.createUnstructEvent, unstructEventFlattenSchemaKeys)
    val unifiedEventJsonObject = (contexts ++ unstructEvents)
      .map(_._1)
      .foldLeft(event.asJsonObject.toMap - "unstruct_event" - "contexts" - "derived_contexts") { (c, i) =>
        c ++ i.asObject.get.toMap
      }
      .asJsonObject
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

  private def createJsonAndSelfDescribingEntityPair(
    f: String => Json,
    flattenSchemaKeys: List[String]
  ): List[(Json, SelfDescribingEntity)] =
    flattenSchemaKeys.map { key =>
      val json = f(key)
      val selfDescribingEntity = SelfDescribingEntity(
        key,
        json.hcursor.downField(key).as[Json].getOrElse(throw new RuntimeException("Value with schema key not found"))
      )
      (json, selfDescribingEntity)
    }

  "parse" should {
    val event = SpecHelpers.events.exampleEvent

    "succeed with minimal payload without any context or unstruct event" in {
      val expected = Right(ReconstructedEvent(event, List()))
      PayloadParser.parse(event.asJsonObject) mustEqual expected
    }

    "succeed with context array which has flattened schema key prefixed with 'contexts'" in {
      val (res, expected) = parseEventWithContext(
        event,
        contextFlattenSchemaKeys = List(flattenSchemaKeyContexts)
      )
      res mustEqual expected
    }

    "succeed with context array which has flattened schema key with random prefix" in {
      val (res, expected) = parseEventWithContext(
        event,
        contextFlattenSchemaKeys = List(flattenSchemaKeyRandomPrefix)
      )
      res mustNotEqual expected
    }

    "succeed with more than one context array which has flattened schema key prefixed with 'contexts'" in {
      val (res, expected) = parseEventWithContext(
        event,
        contextFlattenSchemaKeys = List(
          s"${flattenSchemaKeyContexts}_1",
          s"${flattenSchemaKeyContexts}_2"
        )
      )
      res mustEqual expected
    }

    "succeed with unstruct event which has flattened schema key prefixed with 'unstruct_event'" in {
      val (res, expected) = parseEventWithContext(
        event,
        unstructEventFlattenSchemaKeys = List(flattenSchemaKeyUnstructEvent)
      )
      res mustEqual expected
    }

    "succeed with unstruct event and context array" in {
      val (res, expected) = parseEventWithContext(
        event,
        contextFlattenSchemaKeys       = List(flattenSchemaKeyContexts),
        unstructEventFlattenSchemaKeys = List(flattenSchemaKeyUnstructEvent)
      )
      res mustEqual expected
    }

    "succeed with payload in which one required event field is missing" in {
      val eventJson    = SpecHelpers.events.requiredFieldMissingEventJson
      val eventJsonStr = Payload.RawPayload(eventJson.noSpaces)
      val info = FailureDetails
        .LoaderRecoveryError
        .ParsingError("Attempt to decode value on failed cursor", List("DownField(v_etl)"))
      val failure = Failure.LoaderRecoveryFailure(info)
      PayloadParser.parse(eventJson.asObject.get) match {
        case Left(BadRow.LoaderRecoveryError(_, `failure`, `eventJsonStr`)) => ok
        case _                                                              => ko("Unexpected parsing result")
      }
    }
  }
}
