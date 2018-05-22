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

import com.spotify.scio.bigquery.TableRow

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer.jsonifyGoodEvent

import org.json4s.JsonAST._

object Utils {

  def parse(record: String): Option[TableRow] = {
    jsonifyGoodEvent(record.split("\t", -1)) match {
      case Right((_, json)) => Some(toTableRow(json))
      case Left(errors) => None
    }
  }

  def toTableRow(json: JObject): TableRow = {
    val atomicFields = json.obj.flatMap {
      case (key, JString(str)) => Some((key, str))
      case (key, JInt(int)) => Some((key, int))
      case (key, JDouble(double)) => Some((key, double))
      case (key, JDecimal(decimal)) => Some((key, decimal))
      case (key, JBool(bool)) => Some((key, bool))
      case (_, JNull) => None
      case _ => None
    }
    TableRow(atomicFields: _*)
  }
}
