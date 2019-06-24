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

import io.circe.{Decoder, Json, JsonObject}
import io.circe.parser.parse

import com.permutive.pubsub.consumer.decoder.MessageDecoder

import PayloadParser.ReconstructedEvent

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

  def parsePayload[F[_]: Sync]: F[Either[BadRow, ReconstructedEvent]] =
    Sync[F].delay(PayloadParser.parse(payload))
}

object EventContainer {

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
