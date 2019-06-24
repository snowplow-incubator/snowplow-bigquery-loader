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
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.generic.semiauto._
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
sealed trait BadRow extends Product with Serializable {
  def getSelfDescribingData: SelfDescribingData[Json]
  def compact: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {
  val BQLoaderProcessorInfo = ProcessorInfo(ProjectMetadata.name, ProjectMetadata.version)

  implicit val bqRepeaterBadRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingError(payload, message, location) => json"""{
          "payload": $payload,
          "error": $message,
          "location": $location,
          "processor": $BQLoaderProcessorInfo
        }"""
      case PubSubError(payload, error) => json"""{
          "payload": $payload,
          "error": $error,
          "processor": $BQLoaderProcessorInfo
        }"""
      case BigQueryError(reconstructedEvent, errorInfo) => json"""{
          "reconstructedEvent": $reconstructedEvent,
          "error": $errorInfo,
          "processor": $BQLoaderProcessorInfo
        }"""
    }

  /**
    * Represents situations where payload object can not be
    * converted back to enriched event format successfully
    * @param payload data which is tried to be converted back to enrich event
    * @param message error message
    * @param location location in the JSON object where error happened
    */
  final case class ParsingError(payload: String, message: String, location: List[String]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.RepeaterParsingError, (this: BadRow).asJson)
  }

  /**
    * Represents error which occurs while trying to fetch events
    * from PubSub topic
    * @param payload payload from PubSub
    * @param error error message
    */
  final case class PubSubError(payload: String, error: String) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.RepeaterPubSubError, (this: BadRow).asJson)
  }

  /**
    * Represents errors which occurs while trying to insert the event
    * to BigQuery via BigQuery SDK
    * @param reconstructedEvent event which is tried to be inserted during error
    * @param errorInfo Gives detailed information about error such that location,
    *                  reason and error message
    */
  final case class BigQueryError(reconstructedEvent: ReconstructedEvent, errorInfo: BqErrorInfo) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.RepeaterBQError, (this: BadRow).asJson)
  }

  /**
    * Gives detailed information such that reason, location of error and error
    * message. These infos are deducted from the error object which is got
    * from BQ SDK. Location and reason of error could not be extracted some
    * error objects therefore they are made optional
    * @param reason   optional error reason which can be extracted from the
    *                 error instance which is got from BigQuery SDK
    * @param location optional location info which can be extracted from the
    *                 error instance which is got from BigQuery SDK
    * @param message  error message which is extracted from the error instance
    *                 which is got from BigQuery SDK
    */
  final case class BqErrorInfo(reason: Option[String], location: Option[String], message: String)

  implicit val bqRepeaterBqErrorInfoCirceJsonEncoder: Encoder[BqErrorInfo] = deriveEncoder[BqErrorInfo]

  def fromJava(error: JBigQueryError): BqErrorInfo =
    BqErrorInfo(Option(error.getReason), Option(error.getLocation), error.getMessage)

  def extract(exception: BigQueryException): BqErrorInfo = {
    val default = BqErrorInfo(None, None, exception.getMessage)
    Option(exception.getError).map(fromJava).getOrElse(default)
  }

  def extract(errors: JMap[java.lang.Long, JList[JBigQueryError]]): BqErrorInfo =
    errors
      .asScala
      .toList
      .flatMap(_._2.asScala.toList)
      .headOption
      .map(fromJava)
      .getOrElse(BqErrorInfo(None, None, errors.toString))
}
