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

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError => BigQueryCastError}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.storage.bigquery.common.{BadRowSchemas, ProcessorInfo}

import com.snowplowanalytics.snowplow.storage.bigquery.loader.generated.ProjectMetadata

/**
  * Represents events which are problematic to process due to
  * several reasons such as could not be parsed, error while validate
  * them with Iglu, error while casting them to BigQuery types etc.
  */
sealed trait BadRow {
  def getSelfDescribingData: SelfDescribingData[Json]
  def compact: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {
  val BQLoaderProcessorInfo = ProcessorInfo.Processor(ProjectMetadata.name, ProjectMetadata.version)

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
    * Represents the errors due to something went wrong internally
    * In this kind of errors, event is parsed properly, therefore
    * enriched event is given in the error as properly parsed
    * @param enrichedEvent event which is enriched successfully
    * @param errors        info of errors
    */
  final case class InternalError(enrichedEvent: Event, errors: NonEmptyList[InternalErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderInternalError, (this: BadRow).asJson)
  }

  implicit val badRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingError(payload, errors) => json"""{
          "payload": $payload,
          "errors": $errors,
          "processor": $BQLoaderProcessorInfo
        }"""
      case InternalError(enrichedEvent, errors) => json"""{
          "enrichedEvent": $enrichedEvent,
          "failures": $errors,
          "processor": $BQLoaderProcessorInfo
        }"""
    }

  /**
    * Gives info about the reasons of the internal errors
    */
  sealed trait InternalErrorInfo

  object InternalErrorInfo {

    /**
      * Represents errors which occurs when making validation
      * against a schema with Iglu client
      * @param schemaKey instance of schemaKey which could not be found
      * @param error     instance of ClientError which gives info
      *                  about the reason of lookup error
      */
    final case class IgluValidationError(schemaKey: SchemaKey, error: ClientError) extends InternalErrorInfo

    /**
      * Represents errors which occurs when trying to turn JSON value
      * into BigQuery-compatible row
      * @param data      JSON data which tried to be casted
      * @param schemaKey key of schema which 'Field' instance to cast is created from
      * @param errors    info about reason of the error
      */
    final case class CastError(data: Json, schemaKey: SchemaKey, errors: NonEmptyList[BigQueryCastError]) extends InternalErrorInfo

    final case class UnexpectedError(value: Json, message: String) extends InternalErrorInfo

    implicit val errorInfoJsonEncoder: Encoder[InternalErrorInfo] =
      Encoder.instance {
        case IgluValidationError(schemaKey, error) => json"""{
            "igluValidationError": {
              "schemaKey": $schemaKey,
              "error": $error
            }
          }"""
        case CastError(data, schemaKey, errors) => json"""{
            "castError": {
              "data": $data,
              "schemaKey": $schemaKey,
              "errors": $errors
            }
          }"""
        case UnexpectedError(value, message) => json"""{
            "unexpectedError": {
              "value": $value,
              "message": $message
            }
          }"""
      }

    implicit val bigQueryCastErrorJsonEncoder: Encoder[BigQueryCastError] =
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
}
