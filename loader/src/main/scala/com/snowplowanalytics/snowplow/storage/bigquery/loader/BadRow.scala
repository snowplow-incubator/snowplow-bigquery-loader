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

import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.circe.literal._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError => BigQueryCastError}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.storage.bigquery.common.{BadRowSchemas, ProcessorInfo}
import com.snowplowanalytics.snowplow.storage.bigquery.loader.generated.ProjectMetadata

/**
  * Represents events which are problematic to process due to
  * several reasons such as could not be parsed, error while validating
  * them with Iglu, error while casting them to BigQuery types etc.
  */
sealed trait BadRow extends Product with Serializable {
  def getSelfDescribingData: SelfDescribingData[Json]
  def compact: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {
  val BQLoaderProcessorInfo = ProcessorInfo(ProjectMetadata.name, ProjectMetadata.version)

  implicit val bqLoaderBadRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingError(payload, errors) => json"""{
          "payload": $payload,
          "errors": $errors,
          "processor": $BQLoaderProcessorInfo
        }"""
      case IgluError(original, errorInfos) => json"""{
          "original": $original,
          "errors": $errorInfos,
          "processor": $BQLoaderProcessorInfo
        }"""
      case CastError(original, errorInfos) => json"""{
          "original": $original,
          "errors": $errorInfos,
          "processor": $BQLoaderProcessorInfo
        }"""
      case RuntimeError(original, errorInfos) => json"""{
          "original": $original,
          "errors": $errorInfos,
          "processor": $BQLoaderProcessorInfo
        }"""
    }

  /**
    * Represents the failure case where data can not be parsed as a proper event
    * @param payload data blob tried to be parsed
    * @param errors  errors in the end of the parsing process
    */
  final case class ParsingError(payload: String, errors: NonEmptyList[String]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderParsingError, (this: BadRow).asJson)
  }

  /**
    * Represents errors which occurs when making validation
    * against a schema with Iglu client
    * @param original event which is worked on when error is got
    * @param errorInfos list of error info objects which contains reasons of errors
    */
  final case class IgluError(original: Event, errorInfos: NonEmptyList[IgluErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderIgluError, (this: BadRow).asJson)
  }

  /**
    * Gives information about Iglu error such as which schema to work with
    * and error object which is got from client
    * @param schemaKey key of schema which is worked with when error got
    * @param error error object from Iglu client which gives information about error
    */
  final case class IgluErrorInfo(schemaKey: SchemaKey, error: ClientError)

  implicit val bqLoaderIgluErrorInfoCirceJsonEncoder: Encoder[IgluErrorInfo] = deriveEncoder[IgluErrorInfo]

  /**
    * Represents errors which occurs when trying to turn JSON value
    * into BigQuery-compatible row
    * @param original event which is worked on when error is got
    * @param errorInfos list of error info objects which contains reasons of errors
    */
  final case class CastError(original: Event, errorInfos: NonEmptyList[CastErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.CastError, (this: BadRow).asJson)
  }

  /**
    * Gives information about error which is got while casting data to
    * BigQuery-compatible format.
    * @param data data which tried to be casted during error
    * @param schemaKey key of schema which is Ddl schema for row is created
    * @param errors error objects from SchemaDDL which is the library where
    *               cast operation is done
    */
  final case class CastErrorInfo(data: Json, schemaKey: SchemaKey, errors: NonEmptyList[BigQueryCastError])

  implicit val bqLoaderCastErrorInfoCirceJsonEncoder: Encoder[CastErrorInfo] = deriveEncoder[CastErrorInfo]

  /**
    * Represents errors which are not expected to happen. For example,
    * if some error happens during parsing the schema json to Ddl schema,
    * this is due to some bugs in SchemaDDL library which is unlikely to
    * happen. Therefore, these types error collected under one type of error
    * @param original event which is worked on when error is got
    * @param errorInfos list of error info objects which contains
*                       reasons of errors
    */
  final case class RuntimeError(original: Event, errorInfos: NonEmptyList[RuntimeErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderRuntimeError, (this: BadRow).asJson)
  }

  /**
    * Gives information about unexpected error with bearing some value related
    * with error and error message
    * @param value json value which is given since it is thought related to error
    * @param message error message
    */
  final case class RuntimeErrorInfo(value: Json, message: String)

  implicit val bqLoaderRuntimeErrorInfoCirceJsonEncoder: Encoder[RuntimeErrorInfo] = deriveEncoder[RuntimeErrorInfo]

  implicit val bqLoaderBigQueryCastErrorJsonEncoder: Encoder[BigQueryCastError] =
    Encoder.instance {
      case BigQueryCastError.WrongType(value, expected) => json"""{
            "wrongType": {
              "value": $value,
              "expectedType": ${expected.toString}
            }
          }"""
      case BigQueryCastError.NotAnArray(value, expected) => json"""{
            "notAnArray": {
              "value": $value,
              "expectedType": ${expected.toString}
            }
          }"""
      case BigQueryCastError.MissingInValue(key, value) => json"""{
            "missingInValue": {
              "value": $value,
              "missingKey": $key
            }
          }"""
    }
}
