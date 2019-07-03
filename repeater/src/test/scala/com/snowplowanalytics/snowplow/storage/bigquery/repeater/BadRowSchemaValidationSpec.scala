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
import cats.data.NonEmptyList
import cats.effect.Clock
import io.circe.literal._
import io.circe.syntax._
import io.circe.Json
import com.snowplowanalytics.iglu.client.CirceValidator
import com.snowplowanalytics.iglu.client.Resolver
import BadRow.InternalErrorInfo.{BigQueryError, UnknownError}
import BadRow.{EventParsingError, InternalError, JsonParsingError, ParsingError}
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
               "name":"Iglu Central Mirror",
               "priority":0,
               "vendorPrefixes":[
                 "com.snowplowanalytics"
               ],
               "connection":{
                 "http":{
                  "uri":"http://18.195.229.140"
                 }
               }
              },
              {
                "name":"Local Iglu Central",
                "priority":0,
                "vendorPrefixes":[
                  "com.snowplowanalytics"
                ],
                "connection":{
                  "http":{
                    "uri":"http://localhost:4040"
                  }
                }
              },
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

  val eventParsingErrorGen = for {
    message <- Gen.alphaNumStr
    errors <- Gen.nonEmptyListOf(Gen.alphaNumStr)
  } yield EventParsingError(message, errors)

  val jsonParsingErrorGen = for {
    error <- Gen.alphaNumStr
  } yield JsonParsingError(error)

  val parsingErrorGen = for {
    payload <- Gen.alphaNumStr
    parsingErrorInfoGen <- Gen.oneOf(eventParsingErrorGen, jsonParsingErrorGen)
    parsingErrorInfos <- Gen.nonEmptyListOf(parsingErrorInfoGen)
  } yield ParsingError(payload, NonEmptyList.fromListUnsafe(parsingErrorInfos))

  val selfDescribingEntityGen = for {
    flattenSchemaKey <- Gen.alphaNumStr
  } yield SelfDescribingEntity(flattenSchemaKey, mockJsonValue)

  val reconstructedEventGen = for {
    jsonsWithFlattenSchemaKey <- Gen.listOf(selfDescribingEntityGen)
  } yield ReconstructedEvent(SpecHelpers.exampleMinimalEvent, jsonsWithFlattenSchemaKey)

  val bigQueryErrorGen = for {
    reason <- Gen.alphaNumStr
    location <- Gen.option(Gen.alphaNumStr)
    message <- Gen.alphaNumStr
  } yield BigQueryError(reason, location, message)

  val unknownErrorGen = for {
    message <- Gen.alphaNumStr
  } yield UnknownError(message)

  val internalErrorGen = for {
    reconstructedEvent <- reconstructedEventGen
    internalErrorGen <- Gen.oneOf(bigQueryErrorGen, unknownErrorGen)
    internalErrors <- Gen.nonEmptyListOf(internalErrorGen)
  } yield InternalError(reconstructedEvent, NonEmptyList.fromListUnsafe(internalErrors))

  def validateBadRow(badRow: BadRow) = {
    val badRowSelfDescribingData = badRow.getSelfDescribingData
    val schema = resolver.lookupSchema(badRowSelfDescribingData.schema, 2)
    CirceValidator.validate(badRowSelfDescribingData.data, schema.getOrElse(throw new RuntimeException(s"Schema could not be found: $schema")))
  }
}

class BadRowSchemaValidationSpec extends Specification with ScalaCheck { def is = s2"""
  self describing json of 'parsing error' complies its schema $e1
  self describing json of 'internal error' complies its schema $e2
  """
  import BadRowSchemaValidationSpec._

  def e1 = {
    forAll(parsingErrorGen) {
      parsingFailure => validateBadRow(parsingFailure) must beRight
    }
  }

  def e2 = {
    forAll(internalErrorGen) {
      internalError => validateBadRow(internalError) must beRight
    }
  }
}

