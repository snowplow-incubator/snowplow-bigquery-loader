package com.snowplowanalytics.snowplow.storage.bigquery.common

import io.circe._

import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.option._

import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonDSL._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data._

object Codecs {
  def toPayload(item: InventoryItem): JValue =
    ("schema" -> item.igluUri) ~ ("type" -> item.shredProperty.name.toUpperCase)

  def toPayload(items: Set[InventoryItem]): JValue =
    JArray(items.toList.map(toPayload))

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

  implicit val inventoryListEncoder: Encoder[List[InventoryItem]] =
    Encoder.instance(items => Json.fromValues(items.map(inventoryEncoder.apply)))
}
