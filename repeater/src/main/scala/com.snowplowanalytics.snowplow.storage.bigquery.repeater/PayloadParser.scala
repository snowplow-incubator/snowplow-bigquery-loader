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

import cats.syntax.either._
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import BadRow.ParsingError

/**
  * Parser which is used for reconverting failedInsert payload into
  * enriched event. Since shredded types can not be reconverted into
  * schema key deterministically, payload can not be reconverted
  * into event fully however it can be converted mostly. In order to
  * get more information, look at the comments of "ReconstructedEvent"
  */
object PayloadParser {
  def parse(payload: JsonObject): Either[ParsingError, ReconstructedEvent] =
    payload
      .add("contexts", Json.obj())
      .add("derived_contexts", Json.obj())
      .add("unstruct_event", Json.obj())
      .asJson.as[Event]
      .leftMap { e =>
        ParsingError(
          payload.asJson.noSpaces,
          e.message,
          e.history.foldLeft(List[String]())((l, i) => i.toString::l)
        )
      }
      .map(ReconstructedEvent(_, getSelfDescribingEntities(payload)))

  private def getSelfDescribingEntities(payload: JsonObject): List[SelfDescribingEntity] = {
    val jsonPrefixes = List("contexts", "unstruct_event")
    jsonPrefixes.flatMap(getSelfDescribingEntitiesWithPrefix(payload, _))
  }

  private def getSelfDescribingEntitiesWithPrefix(payload: JsonObject, prefix: String): List[SelfDescribingEntity] =
    payload.filter {
      case (key, value) => key.startsWith(prefix) && value != Json.obj() && value != Json.Null
    }
    .toList.map {
      case (shreddedType, data) => SelfDescribingEntity(shreddedType, data)
    }

  /**
    * Represents event which is reconstructed from payload of the failed insert
    * It consists of two part because schema keys of the self describing JSONs
    * such as unstruct_event, contexts or derived_contexts are shredded in the
    * payload. For example schema key with com.snowplowanalytics.snowplow as
    * vendor, web_page as name, 1.0.0 as version is converted to
    * "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0" and this key could
    * not be reconverted to its original schema key deterministically. Therefore,
    * they can not be behaved like self describing JSONs after reconstruction because
    * their schema key are unknown. Therefore, reconstructed event split into two part
    * as atomic and self describing entities
    * @param atomic event instance which only consists of atomic event parts
    * @param selfDescribingEntities list of SelfDescribingEntity instances
    *        they consist of reconstructed self describing JSONs from the payload
    */
  final case class ReconstructedEvent(atomic: Event, selfDescribingEntities: List[SelfDescribingEntity])

  implicit val reconstructedEventEncoder: Encoder[ReconstructedEvent] = deriveEncoder[ReconstructedEvent]

  /**
    * Represents self describing entities with shredded type
    * @param shreddedType shredded type e.g. "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
    * @param data JSON object which can be unstruct_event, array of contexts or array of derived_contexts
    */
  final case class SelfDescribingEntity(shreddedType: String, data: Json)

  implicit val selfDescribingEntityEncoder: Encoder[SelfDescribingEntity] = deriveEncoder[SelfDescribingEntity]
}
