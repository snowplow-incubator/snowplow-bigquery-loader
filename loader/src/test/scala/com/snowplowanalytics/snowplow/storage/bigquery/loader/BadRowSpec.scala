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

import cats.data.NonEmptyList
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.snowplow.storage.bigquery.loader.BadRow._
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

}

class BadRowSpec extends Specification { def is = s2"""
  parsing not enriched event returns error $e1
  fromEvent returns IgluError if event has contexts with not existing schemas $e2
  fromEvent returns more than one IgluError in case of multiple not existing schemas $e3
  fromEvent returns CastError if event has missing fields with respect to its schema $e4
  """
  import BadRowSpec._

  def e1 = {
    val res = LoaderRow.parse(BadRowSpec.resolverConfig)(notEnrichedEvent)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beEventParsingErrorAndHaveSamePayload(notEnrichedEvent)
  }

  def e2 = {
    val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "not_existing_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val contexts = List(
      SelfDescribingData(
        schemaKey,
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event) match {
      case Left(IgluError(`event`, NonEmptyList(IgluErrorInfo(`schemaKey`, _), List()))) => true
      case _ => false
    }
    res must beTrue
  }

  def e3 = {
    val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "not_existing_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val schemaKey2 = SchemaKey("com.snowplowanalytics.snowplow", "not_existing_context_2", "jsonschema", SchemaVer.Full(1, 0, 0))
    val contexts = List(
      SelfDescribingData(
        schemaKey,
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      ),
      SelfDescribingData(
        schemaKey2,
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event) match {
      case Left(IgluError(`event`, NonEmptyList(IgluErrorInfo(`schemaKey2`, _), List(IgluErrorInfo(`schemaKey`, _))))) => true
      case _ => false
    }
    res must beTrue
  }

  def e4 = {
    val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val contexts = List(
      SelfDescribingData(
        schemaKey,
        json"""{}"""
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event) match {
      case Left(CastError(`event`, NonEmptyList(CastErrorInfo(_, `schemaKey`, _), _))) => true
      case _ => false
    }
    res must beTrue
  }
}
