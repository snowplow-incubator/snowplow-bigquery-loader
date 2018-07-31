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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import org.joda.time.Instant

import cats.implicits._

import org.json4s.JsonAST._

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.{InventoryItem, UnstructEvent}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer.getValidatedJsonEvent

import com.spotify.scio.bigquery.TableRow

import common.{Schema, Utils => CommonUtils}

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[InventoryItem])

object LoaderRow {

  def parse(record: String): Option[LoaderRow] = {
    val loaderRow: Either[List[String], LoaderRow] = for {
      (inventory, event) <- getValidatedJsonEvent(record.split("\t", -1), false): Either[List[String], (Set[InventoryItem], JObject)]
      rowWithTstamp      <- toTableRow(event)
      (row, tstamp)       = rowWithTstamp
    } yield LoaderRow(tstamp, row, inventory)

    loaderRow.toOption
  }

  // TODO: enforce Schema produced by Schema DDL
  def toTableRow(json: JObject): Either[List[String], (TableRow, Instant)] = {
    json.obj.collectFirst { case ("collector_tstamp", JString(tstamp)) => tstamp } match {
      case Some(tstamp) =>
        val atomicFields = json.obj.flatMap {
          case ("geo_location", _) => Nil
          case (key, JString(str)) => List((key, str))
          case (key, JInt(int)) => List((key, int))
          case (key, JDouble(double)) => List((key, double))
          case (key, JDecimal(decimal)) => List((key, decimal))
          case (key, JBool(bool)) => List((key, bool))
          case (key, o: JObject) if key.startsWith("unstruct_event") =>
            getUnstructEventField(key, o)
          case (key, a: JArray) if key.startsWith("contexts_") =>
            getContextsField(key, a)
          case (_, JNull) => Nil
          case (_, _: JObject) => Nil    // TODO: warning
          case (_, _: JArray) => Nil     // TODO: warning
          case (_, JNothing) => Nil
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

  def getUnstructEventField(key: String, json: JObject): List[(String, Any)] = {
    CommonUtils.toCirce(json).toData.toList.map { data =>
      val columnName = Schema.getColumnName(InventoryItem(UnstructEvent, data.schema.toSchemaUri))
      val payload = data.data.noSpaces
      (columnName, payload)
    }
  }

  def getContextsField(key: String, json: JArray): List[(String, Any)] = {
    val grouped = json.arr.map(CommonUtils.toCirce).flatMap(_.toData).groupBy(_.schema).flatMap { case (key, datas) =>
      val columnName = Schema.getColumnName(InventoryItem(UnstructEvent, key.toSchemaUri))
      datas.map(_.data).map { data => (columnName, data.noSpaces) }
    }
    grouped.toList
  }
}
