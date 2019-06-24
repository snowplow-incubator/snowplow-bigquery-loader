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

import java.util.concurrent.TimeUnit
import cats.Id
import cats.effect.Clock
import io.circe.literal._
import io.circe.syntax._
import io.circe.Json
import com.snowplowanalytics.iglu.client.CirceValidator
import com.snowplowanalytics.iglu.client.Resolver
import BadRow._
import PayloadParser.{SelfDescribingEntity, ReconstructedEvent}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.specs2.{ScalaCheck, Specification}

object BadRowSchemaValidationSpec {

  implicit val idClock: Clock[Id] = new Clock[Id] {
    def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

    def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }

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

  val resolver = Resolver.parse(resolverConfig).fold(e => throw new RuntimeException(e.toString), identity)

  val mockJsonValue = Json.obj("mockJsonKey" := "mockJsonValue")

  val parsingErrorGen = for {
    payload <- Gen.alphaNumStr
    message <- Gen.alphaNumStr
    location <- Gen.nonEmptyListOf(Gen.alphaNumStr)
  } yield ParsingError(payload, message, location)

  val pubsubErrorGen = for {
    payload <- Gen.alphaNumStr
    error <- Gen.alphaNumStr
  } yield PubSubError(payload, error)

  val selfDescribingEntityGen = Gen.alphaNumStr.map(SelfDescribingEntity(_, mockJsonValue))

  val reconstructedEventGen = Gen.listOf(selfDescribingEntityGen).map(ReconstructedEvent(SpecHelpers.exampleMinimalEvent, _))

  val bigQueryErrorInfoGen = for {
    reason <- Gen.option(Gen.alphaNumStr)
    location <- Gen.option(Gen.alphaNumStr)
    message <- Gen.alphaNumStr
  } yield BqErrorInfo(reason, location, message)

  val bigQueryErrorGen = for {
    reconstructedEvent <- reconstructedEventGen
    bqErrorInfo <- bigQueryErrorInfoGen
  } yield BigQueryError(reconstructedEvent, bqErrorInfo)

  def validateBadRow(badRow: BadRow) = {
    val badRowSelfDescribingData = badRow.getSelfDescribingData
    val schema = resolver.lookupSchema(badRowSelfDescribingData.schema)
    CirceValidator.validate(badRowSelfDescribingData.data, schema.getOrElse(throw new RuntimeException(s"Schema could not be found: $schema")))
  }
}

class BadRowSchemaValidationSpec extends Specification with ScalaCheck { def is = s2"""
  self describing json of 'parsing error' complies its schema $e1
  self describing json of 'pubsub error' complies its schema $e2
  self describing json of 'bigquery error' complies its schema $e3
  """
  import BadRowSchemaValidationSpec._

  def e1 = {
    forAll(parsingErrorGen) {
      parsingFailure => validateBadRow(parsingFailure) must beRight
    }
  }

  def e2 = {
    forAll(pubsubErrorGen) {
      pubsubError => validateBadRow(pubsubError) must beRight
    }
  }

  def e3 = {
    forAll(bigQueryErrorGen) {
      bqErrorGen => validateBadRow(bqErrorGen) must beRight
    }
  }
}

