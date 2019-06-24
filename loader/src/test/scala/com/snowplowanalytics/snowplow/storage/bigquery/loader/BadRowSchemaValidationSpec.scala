/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
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
import io.circe.syntax._
import io.circe.Json
import com.snowplowanalytics.iglu.client.ClientError.ValidationError
import com.snowplowanalytics.iglu.client.validator.ValidatorError.InvalidData
import com.snowplowanalytics.iglu.client.validator.ValidatorReport
import com.snowplowanalytics.iglu.client.{CirceValidator, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.bigquery.CastError.{MissingInValue, NotAnArray, WrongType}
import com.snowplowanalytics.iglu.schemaddl.bigquery.Type
import IdInstances._
import BadRow._
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.specs2.{ScalaCheck, Specification}

object BadRowSchemaValidationSpec {

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
    errors <- Gen.nonEmptyListOf(Gen.alphaNumStr)
  } yield ParsingError(payload, NonEmptyList.fromListUnsafe(errors))

  val schemaVerGen = for {
    model <- Gen.chooseNum(1, 9)
    revision <- Gen.chooseNum(0, 9)
    addition <- Gen.chooseNum(0, 9)
  } yield SchemaVer.Full(model, revision, addition)

  val schemaKeyGen = for {
    vendor <- Gen.identifier
    name <- Gen.identifier
    format <- Gen.identifier
    version <- schemaVerGen
  } yield SchemaKey(vendor, name, format, version)

  val igluErrorInfoGen = schemaKeyGen.map { schemaKey =>
    IgluErrorInfo(
      schemaKey,
      ValidationError(InvalidData(NonEmptyList.of(ValidatorReport("message", None, List(), None))))
    )
  }

  val igluErrorGen = Gen.nonEmptyListOf(igluErrorInfoGen).map { igluErrorInfos =>
    IgluError(SpecHelpers.ExampleEvent, NonEmptyList.fromListUnsafe(igluErrorInfos))
  }

  val bqFieldTypeGen = Gen.oneOf(Type.Boolean, Type.Float, Type.Integer)

  val wrongTypeCastErrorGen = bqFieldTypeGen.map(WrongType(mockJsonValue, _))

  val notAnArrayCastErrorGen = bqFieldTypeGen.map(NotAnArray(mockJsonValue, _))

  val missingInValueCastErrorGen = Gen.alphaNumStr.map(MissingInValue(_, mockJsonValue))

  val castErrorInfoGen = for {
    schemaKey <- schemaKeyGen
    bqCastErrorGen <- Gen.oneOf(wrongTypeCastErrorGen, notAnArrayCastErrorGen, missingInValueCastErrorGen)
    bqCastErrors <- Gen.nonEmptyListOf(bqCastErrorGen)
  } yield CastErrorInfo(mockJsonValue, schemaKey, NonEmptyList.fromListUnsafe(bqCastErrors))

  val castErrorGen = for {
    castErrorInfos <- Gen.nonEmptyListOf(castErrorInfoGen)
  } yield CastError(SpecHelpers.ExampleEvent, NonEmptyList.fromListUnsafe(castErrorInfos))

  val runtimeErrorInfoGen = Gen.alphaNumStr.map(RuntimeErrorInfo(mockJsonValue, _))

  val runtimeErrorGen = Gen.nonEmptyListOf(runtimeErrorInfoGen).map { unexpectedErrorInfos =>
    RuntimeError(SpecHelpers.ExampleEvent, NonEmptyList.fromListUnsafe(unexpectedErrorInfos))
  }

  def validateBadRow(badRow: BadRow) = {
    val badRowSelfDescribingData = badRow.getSelfDescribingData
    val schema = resolver.lookupSchema(badRowSelfDescribingData.schema)
    CirceValidator.validate(badRowSelfDescribingData.data, schema.getOrElse(throw new RuntimeException(s"Schema could not be found: $schema")))
  }
}

class BadRowSchemaValidationSpec extends Specification with ScalaCheck { def is = s2"""
  self describing json of 'parsing error' complies its schema $e1
  self describing json of 'loader iglu error' complies its schema $e2
  self describing json of 'cast error' complies its schema $e3
  self describing json of 'runtime error' complies its schema $e4
  """
  import BadRowSchemaValidationSpec._

  def e1 = {
    forAll(parsingErrorGen) {
      parsingFailure => validateBadRow(parsingFailure) must beRight
    }
  }

  def e2 = {
    forAll(igluErrorGen) {
      igluError => validateBadRow(igluError) must beRight
    }
  }

  def e3 = {
    forAll(castErrorGen) {
      castError => validateBadRow(castError) must beRight
    }
  }

  def e4 = {
    forAll(runtimeErrorGen) {
      runtimeError => validateBadRow(runtimeError) must beRight
    }
  }
}
