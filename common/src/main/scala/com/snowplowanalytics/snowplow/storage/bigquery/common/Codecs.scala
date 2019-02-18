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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import io.circe._

import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.option._

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._

object Codecs {
  def toPayload(item: ShreddedType): Json =
    Json.fromFields(List(
      "schema" -> Json.fromString(item.schemaKey.toSchemaUri),
      "type" -> Json.fromString(item.shredProperty.name.toUpperCase)
    ))

  def toPayload(items: Set[ShreddedType]): Json =
    Json.fromValues(items.toList.map(toPayload))

  private val ContextsName = "CONTEXTS"
  private val DerivedContextsName = "DERIVED_CONTEXTS"
  private val UnstructEventName = "UNSTRUCT_EVENT"

  val ValidProperties = List(ContextsName, DerivedContextsName, UnstructEventName).mkString(", ")

  def decodeShredProperty(string: String): Either[String, ShredProperty] = string match {
    case ContextsName => Contexts(CustomContexts).asRight
    case DerivedContextsName => Contexts(DerivedContexts).asRight
    case UnstructEventName => UnstructEvent.asRight
    case other => Left(s"[$other] is not a valid shred property; valid are: $ValidProperties")
  }

  implicit val shredPropertyDecoder: Decoder[ShredProperty] =
    new Decoder[ShredProperty] {
      def apply(c: HCursor): Decoder.Result[ShredProperty] =
        c.value
          .asString
          .map(string => decodeShredProperty(string).leftMap(message => DecodingFailure(message, c.history)))
          .getOrElse(DecodingFailure("Not a string", c.history).asLeft)
    }

  implicit val shredPropertyEncoder: Encoder[ShredProperty] =
    new Encoder[ShredProperty] {
      def apply(a: ShredProperty): Json = a match {
        case Contexts(CustomContexts) => Json.fromString(ContextsName)
        case Contexts(DerivedContexts) => Json.fromString(DerivedContextsName)
        case UnstructEvent => Json.fromString(UnstructEventName)
      }
    }

  implicit val inventoryDecoder: Decoder[ShreddedType] =
    new Decoder[ShreddedType] {
      def apply(c: HCursor): Decoder.Result[ShreddedType] =
        c.value.asObject match {
          case Some(obj) =>
            val map = obj.toMap
            for {
              schema <- map.get("schema")
                .liftTo[Decoder.Result](DecodingFailure("Property \"schema\" is not present", c.history))
              schemaStr <- schema.asString
                .liftTo[Decoder.Result](DecodingFailure("Property \"schema\" is not a string", c.history)).flatMap {
                x => SchemaKey.fromUri(x).leftMap(e => DecodingFailure(e.code, Nil))
              }
              shredPropertyStr <- map
                .get("type").liftTo[Decoder.Result](DecodingFailure("Property \"type\" is not present", c.history))
              shredProperty <- shredPropertyStr.as[ShredProperty]
            } yield ShreddedType(shredProperty, schemaStr)
          case None =>
            DecodingFailure(s"Cannot decode InventoryItem, ${c.value} is not an object", c.history).asLeft
        }
    }

  implicit val inventoryEncoder: Encoder[ShreddedType] =
    new Encoder[ShreddedType] {
      def apply(a: ShreddedType): Json = {
        val items = List(
          "schema" -> Json.fromString(a.schemaKey.toSchemaUri),
          "type" -> shredPropertyEncoder(a.shredProperty)
        )

        Json.fromJsonObject(JsonObject.fromFoldable(items))
      }
    }

  implicit val inventoryListEncoder: Encoder[List[ShreddedType]] =
    Encoder.instance(items => Json.fromValues(items.map(inventoryEncoder.apply)))
}
