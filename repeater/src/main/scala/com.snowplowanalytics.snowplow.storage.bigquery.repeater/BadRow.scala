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

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import cats.data.NonEmptyList
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.circe.literal._
import com.google.cloud.bigquery.{BigQueryException, BigQueryError => JBigQueryError}
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.storage.bigquery.common.{BadRowSchemas, ProcessorInfo}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.PayloadParser.ReconstructedEvent
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.generated.ProjectMetadata


/**
  * Represents rows of failed inserts which are problematic
  * to process due to several reasons such as could not be
  * converted back to enrich event, error while trying to
  * insert row to BigQuery etc
  */
sealed trait BadRow {
  def getSelfDescribingData: SelfDescribingData[Json]
  def compact: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {
  val BQLoaderProcessorInfo = ProcessorInfo.Processor(ProjectMetadata.name, ProjectMetadata.version)

  /**
    * Represents situations where payload object can not be
    * converted back to enriched event format successfully
    * @param payload data which is tried to be converted back to enrich event
    * @param errors gives info about reasons of failure to convert it back
    */
  final case class ParsingError(payload: String, errors: NonEmptyList[ParsingErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.RepeaterParsingError, (this: BadRow).asJson)
  }

  /**
    * Gives info about reasons of parsing error
    */
  sealed trait ParsingErrorInfo

  /**
    * Gives info about reasons of failure to parse payload to event
    * @param message error message
    * @param location location in the JSON object where error happened
    */
  final case class EventParsingError(message: String, location: List[String]) extends ParsingErrorInfo

  /**
    * Gives info about reasons of failure to parse payload to json
    * @param error error message
    */
  final case class JsonParsingError(error: String) extends ParsingErrorInfo

  implicit val parsingErrorInfoEncoder: Encoder[ParsingErrorInfo] =
    Encoder.instance {
      case EventParsingError(message, location) => json"""{
          "eventParsingError": {
            "error": $message,
            "location": $location
          }
        }"""
      case JsonParsingError(error) => json"""{
          "jsonParsingError": {
            "error": $error
          }
        }"""
    }

  /**
    * Represents situations where something went wrong internally
    * In this kind of situations, payload data can be successfully
    * converted back to enrich event format but some internal error
    * happened while trying to insert the event to BiqQuery
    * @param reconstructedEvent event which reconstructed from JSON payload
    * @param errors gives info about reasons of internal error
    */
  final case class InternalError(reconstructedEvent: ReconstructedEvent, errors: NonEmptyList[InternalErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.RepeaterInternalError, (this: BadRow).asJson)
  }

  implicit val badRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingError(payload, errors) => json"""{
          "payload": $payload,
          "errors": $errors,
          "processor": $BQLoaderProcessorInfo
        }"""
      case InternalError(reconstructedEvent, errors) => json"""{
          "event": $reconstructedEvent,
          "failures": $errors,
          "processor": $BQLoaderProcessorInfo
        }"""
    }


  /**
    * Gives info about reasons of internal error
    */
  sealed trait InternalErrorInfo

  object InternalErrorInfo {

    /**
      * Represents errors which occurs while trying to insert the event
      * to BigQuery via BigQuery SDK
      * @param reason   error reason which is extracted from the error instance
      *                 which is got from BigQuery SDK
      * @param location location info which is extracted from the error instance
      *                 which is got from BigQuery SDK
      * @param message  error message which is extracted from the error instance
      *                 which is got from BigQuery SDK
      */
    final case class BigQueryError(reason: String, location: Option[String], message: String) extends InternalErrorInfo

    /**
      * Represents errors which occurs while trying to insert the event
      * to BigQuery however no info can be extracted from the error object which
      * is got from BigQuery SDK
      * @param message error message
      */
    final case class UnknownError(message: String) extends InternalErrorInfo

    implicit val errorInfoJsonEncoder: Encoder[InternalErrorInfo] =
      Encoder.instance {
        case BigQueryError(reason, location, message) => json"""{
            "bigQueryError": {
              "reason": $reason,
              "location": $location,
              "message": $message
            }
          }"""
        case UnknownError(message) => json"""{
            "unknownError": {
              "message": $message
            }
          }"""
      }
  }


  def fromJava(error: JBigQueryError): InternalErrorInfo =
    InternalErrorInfo.BigQueryError(error.getReason, Option(error.getLocation), error.getMessage)

  def extract(exception: BigQueryException): InternalErrorInfo = {
    val default = InternalErrorInfo.UnknownError(exception.getMessage)
    Option(exception.getError).map(fromJava).getOrElse(default)
  }

  def extract(errors: JMap[java.lang.Long, JList[JBigQueryError]]): InternalErrorInfo =
    errors
      .asScala
      .toList
      .flatMap(_._2.asScala.toList)
      .headOption
      .map(fromJava)
      .getOrElse(InternalErrorInfo.UnknownError(errors.toString))
}
