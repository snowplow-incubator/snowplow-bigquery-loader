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

import java.time.Instant
import java.util
import java.util.{UUID, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import cats.syntax.either._
import cats.effect.Sync

import com.google.cloud.bigquery.{BigQueryException, BigQueryError => JBigQueryError}

import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.parser.parse
import io.circe.generic.semiauto._

import com.permutive.pubsub.consumer.decoder.MessageDecoder

/**
  * Primary data type for events parsed from `failedInserts` PubSub subscription
  * (which should be an enriched event)
  */
case class EventContainer(eventId: UUID, etlTstamp: Instant, payload: JsonObject) {
  /** Transform the scala class into Java Map, understandable by BigQuery client */
  def decompose: JMap[String, Any] = EventContainer.decomposeObject(payload)

  /** Check if event is older than `seconds` (time to create a column) */
  def isReady[F[_]: Sync](seconds: Long): F[Boolean] =
    Sync[F].delay(etlTstamp.isBefore(Instant.now().minusSeconds(seconds)))
}

object EventContainer {

  /**
    * The last incarnation of enriched event, that can go only into GCS bucket
    * @param payload the original enriched event payload, produced by BQ Loader (Analtyics SDK)
    * @param error happened during repeat (retry insert)
    */
  case class Desperate(payload: JsonObject, error: FailedRetry)

  object Desperate {
    implicit def circeDesperateEncoder: Encoder[Desperate] =
      deriveEncoder[Desperate]
  }

  /** Result of repeat (retry insert) */
  sealed trait FailedRetry
  object FailedRetry {
    case class BigQueryError(reason: String, location: Option[String], message: String) extends FailedRetry
    case class UnknownError(message: String) extends FailedRetry

    def fromJava(error: JBigQueryError): BigQueryError =
      BigQueryError(error.getReason, Option(error.getLocation), error.getMessage)

    def extract(exception: BigQueryException): FailedRetry = {
      val default = BigQueryError("Unknown reason", None, exception.getMessage)
      Option(exception.getError).map(fromJava).getOrElse(default)
    }

    def extract(errors: JMap[java.lang.Long, JList[JBigQueryError]]): FailedRetry =
      errors
        .asScala
        .toList
        .flatMap(_._2.asScala.toList)
        .headOption
        .map(fromJava)
        .getOrElse(UnknownError(errors.toString))

    implicit def circeFailedRetryEncoder: Encoder[FailedRetry] =
      deriveEncoder[FailedRetry]
  }

  implicit val circeEventDecoder: Decoder[EventContainer] = Decoder.instance { cursor =>
    for {
      jsonObject <- cursor.as[JsonObject]
      eventId    <- cursor.get[UUID]("event_id")
      etlTstamp  <- cursor.get[Instant]("etl_tstamp")
    } yield EventContainer(eventId, etlTstamp, jsonObject)
  }

  implicit val pubsubEventDecoder: MessageDecoder[EventContainer] = { bytes: Array[Byte] =>
    for {
      string  <- Either.catchNonFatal(new String(bytes))
      json    <- parse(string)
      payload <- json.as[EventContainer]
    } yield payload
  }

  private def decomposeArray(arr: Vector[Json]): JList[Any] =
    arr.map(decomposeJson).asJava

  private def decomposeObject(json: JsonObject): JMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    json.toMap.foreach { case (k, v) =>
      map.put(k, decomposeJson(v))
    }
    map
  }

  private def decomposeJson(json: Json): Any = {
    json.fold(
      null,
      b => b,
      i => i.toInt.orElse(i.toBigInt.map(_.bigInteger)).getOrElse(i.toDouble),
      s => s,
      a => decomposeArray(a),
      o => decomposeObject(o)
    )
  }

}
