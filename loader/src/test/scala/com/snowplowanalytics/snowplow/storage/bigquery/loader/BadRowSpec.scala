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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import scala.reflect.{ClassTag, classTag}
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.snowplow.storage.bigquery.loader.BadRow.{InternalError, InternalErrorInfo, ParsingError}
import org.specs2.Specification
import org.specs2.matcher.{Expectable, MatchFailure, MatchResult, MatchSuccess, Matcher}

object BadRowSpec {

  val notEnrichedEvent = "Not enriched event"

  val resolverConfig = json"""
      {
         "schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
         "data":{
            "cacheSize":500,
            "repositories":[
               {
                  "name":"Iglu Central",
                  "priority":0,
                  "vendorPrefixes":[
                     "com.snowplowanalytics"
                  ],
                  "connection":{
                     "http":{
                        "uri":"http://iglucentral.com"
                     }
                  }
               }
            ]
         }
      }
    """

  def beEventParsingErrorAndHaveSamePayload(record: String): Matcher[BadRow] = new Matcher[BadRow] {
    override def apply[S <: BadRow](t: Expectable[S]): MatchResult[S] = {
      t.value match {
        case ParsingError(`record`, _) =>
          MatchSuccess("Event parsing errors have same payload", "", t)
        case ParsingError(_, _) =>
          MatchFailure("", "Event parsing errors have different payload", t)
        case _ =>
          MatchFailure("", "Given bad row is not EventParsingError", t)
      }
    }
  }


  def beTypeOfErrorAndHaveSamePayload[T <: InternalErrorInfo: ClassTag](event: Event): Matcher[InternalError] = new Matcher[InternalError] {
    override def apply[S <: InternalError](t: Expectable[S]): MatchResult[S] = t.value match {
      case InternalError(`event`, errorInfos) =>
        val res = errorInfos.foldLeft(true)((res, e) => res & classTag[T].runtimeClass.isInstance(e))
        Matcher.result(
          res,
          "All the errors are IgluValidationError and payload is equal to given event",
          "All the errors are not IgluValidationError",
          t)
      case _ => MatchFailure("", "Given bad row is not BigQueryError", t)
    }
  }
}

class BadRowSpec extends Specification { def is = s2"""
  parsing not enriched event returns error $e1
  fromEvent returns IgluValidationError if event has contexts with not existing schemas $e2
  fromEvent returns CastError if event has missing fields with respect to its schema $e3
  """
  import BadRowSpec._

  def e1 = {
    val res = LoaderRow.parse(BadRowSpec.resolverConfig)(notEnrichedEvent)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beEventParsingErrorAndHaveSamePayload(notEnrichedEvent)
  }

  def e2 = {
    val contexts = List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "not_existing_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beTypeOfErrorAndHaveSamePayload[InternalErrorInfo.IgluValidationError](event)
  }

  def e3 = {
    val contexts = List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{}"""
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beTypeOfErrorAndHaveSamePayload[InternalErrorInfo.CastError](event)
  }
}
