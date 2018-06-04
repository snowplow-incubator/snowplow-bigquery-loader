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
package com.snowplowanalytics.snowplow.storage.bqloader

import org.joda.time.Instant

import cats.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.{EventWithInventory, InventoryItem}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer.transformWithInventory

import com.spotify.scio.bigquery.TableRow

import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{parse => jsonParse}

object Utils {

  def toPayload(item: InventoryItem): JValue =
    ("schema" -> item.igluUri) ~ ("type" -> item.shredProperty.name.toUpperCase)


  case class WindowedRow(start: Instant,
                         end: Instant,
                         atomic: TableRow,
                         inventory: Set[InventoryItem],
                         original: JObject)

  case class LoaderRow(collectorTstamp: Instant,
                       atomic: TableRow,
                       inventory: Set[InventoryItem],
                       original: JObject)

  def parse(record: String): Option[LoaderRow] = {
    print("record! ")
    val loaderRow: Either[List[String], LoaderRow] = for {
      eventWithInventory <- transformWithInventory(record): Either[List[String], EventWithInventory]
      jsonObject          = jsonParse(eventWithInventory.event).asInstanceOf[JObject]
      rowWithTstamp      <- toTableRow(jsonObject)
      (row, tstamp)       = rowWithTstamp
    } yield LoaderRow(tstamp, row, eventWithInventory.inventory, jsonObject)

    loaderRow.toOption
  }

  def toTableRow(json: JObject): Either[List[String], (TableRow, Instant)] = {
    json.obj.collectFirst { case ("collector_tstamp", JString(tstamp)) => tstamp } match {
      case Some(tstamp) =>
        val atomicFields = json.obj.flatMap {
          case (key, JString(str)) => Some((key, str))
          case (key, JInt(int)) => Some((key, int))
          case (key, JDouble(double)) => Some((key, double))
          case (key, JDecimal(decimal)) => Some((key, decimal))
          case (key, JBool(bool)) => Some((key, bool))
          case (_, JNull) => None
          case _ => None
        }
        for {
          time <- Either
            .catchNonFatal(Instant.parse(tstamp))
            .leftMap(err => List(s"Cannot extract collect_tstamp: ${err.getMessage}"))
        } yield (TableRow(atomicFields: _*), time)

      case None =>
        Left(List("No collector_tstamp"))
    }
  }
}
