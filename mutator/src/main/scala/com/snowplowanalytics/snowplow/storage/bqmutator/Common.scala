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
package com.snowplowanalytics.snowplow.storage.bqmutator

import io.circe._

import scalaz.{Failure, Success, ValidationNel}

import cats.data.{Validated, ValidatedNel, NonEmptyList}
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.option._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data._

object Common {
  def fromValidation[E, A](validation: ValidationNel[E, A]): Either[NonEmptyList[E], A] =
    validation match {
      case Success(a) => a.asRight
      case Failure(errors) => NonEmptyList.fromListUnsafe(errors.list).asLeft
    }

  def catchNonFatalMessage[A](a: => A): ValidatedNel[String, A] =
    Validated
      .catchNonFatal(a)
      .leftMap(_.getMessage)
      .toValidatedNel

  implicit val shredPropertyDecoder: Decoder[ShredProperty] =
    new Decoder[ShredProperty] {
      def apply(c: HCursor): Decoder.Result[ShredProperty] = c.value.asString match {
        case Some("CONTEXTS") => Contexts(CustomContexts).asRight
        case Some("DERIVED_CONTEXTS") => Contexts(DerivedContexts).asRight
        case Some("UNSTRUCT_EVENT") => UnstructEvent.asRight
        case Some(other) => DecodingFailure(s"[$other] is not valid shred property", c.history).asLeft
        case None => DecodingFailure("Not a string", c.history).asLeft
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

}
