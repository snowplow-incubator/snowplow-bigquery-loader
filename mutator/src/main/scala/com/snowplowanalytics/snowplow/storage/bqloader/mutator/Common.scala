/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bqloader.mutator

import io.circe._

import cats.data.{Validated, ValidatedNel, NonEmptyList}
import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.option._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data._

object Common {

  private val ContextsName = "CONTEXTS"
  private val DerivedContextsName = "DERIVED_CONTEXTS"
  private val UnstructEventName = "UNSTRUCT_EVENT"

  implicit val shredPropertyDecoder: Decoder[ShredProperty] =
    new Decoder[ShredProperty] {
      def apply(c: HCursor): Decoder.Result[ShredProperty] = c.value.asString match {
        case Some(ContextsName) => Contexts(CustomContexts).asRight
        case Some(DerivedContextsName) => Contexts(DerivedContexts).asRight
        case Some(UnstructEventName) => UnstructEvent.asRight
        case Some(other) => DecodingFailure(s"[$other] is not valid shred property", c.history).asLeft
        case None => DecodingFailure("Not a string", c.history).asLeft
      }
    }

  implicit val shredPropertyEncoder: Encoder[ShredProperty] =
    new Encoder[ShredProperty] {
      def apply(a: ShredProperty): Json = a match {
        case Contexts(CustomContexts) => Json.fromString(ContextsName)
        case Contexts(DerivedContexts) => Json.fromString(DerivedContextsName)
        case UnstructEvent => Json.fromString(UnstructEventName)
      }
    }

  implicit val inventoryDecoder: Decoder[InventoryItem] =
    new Decoder[InventoryItem] {
      def apply(c: HCursor): Decoder.Result[InventoryItem] =
        c.value.asObject match {
          case Some(obj) =>
            val map = obj.toMap
            for {
              schema <- map.get("schema").liftTo[Decoder.Result](DecodingFailure("Property \"schema\" is not present", c.history))
              schemaStr <- schema.asString.liftTo[Decoder.Result](DecodingFailure("Property \"schema\" is not a string", c.history))
              shredPropertyStr <- map.get("type").liftTo[Decoder.Result](DecodingFailure("Property \"type\" is not present", c.history))
              shredProperty <- shredPropertyStr.as[ShredProperty]
            } yield InventoryItem(shredProperty, schemaStr)
          case None =>
            DecodingFailure(s"Cannot decode InventoryItem, ${c.value} is not an object", c.history).asLeft
        }
    }

  implicit val inventoryEncoder: Encoder[InventoryItem] =
    new Encoder[InventoryItem] {
      def apply(a: InventoryItem): Json = {
        val items = List(
          "schema" -> Json.fromString(a.igluUri),
          "type" -> shredPropertyEncoder(a.shredProperty)
        )

        Json.fromJsonObject(JsonObject.fromFoldable(items))
      }
    }
}
