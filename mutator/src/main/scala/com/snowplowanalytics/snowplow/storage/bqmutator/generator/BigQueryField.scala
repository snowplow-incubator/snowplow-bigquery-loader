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
package com.snowplowanalytics.snowplow.storage.bqmutator.generator

import BigQueryField._

/**
  * Type-safe proxy to `Field`
  */
case class BigQueryField(name: String, bigQueryType: BigQueryType, mode: FieldMode) {
  def setMode(mode: FieldMode): BigQueryField = this match {
    case BigQueryField(n, t, _) => BigQueryField(n, t, mode)
  }
}

object BigQueryField {
  sealed trait BigQueryType extends Product with Serializable
  object BigQueryType {
    case object String extends BigQueryType
    case object Boolean extends BigQueryType
    case object Bytes extends BigQueryType
    case object Integer extends BigQueryType
    case object Float extends BigQueryType
    case object Date extends BigQueryType
    case object DateTime extends BigQueryType
    case class Record(fields: List[BigQueryField]) extends BigQueryType
  }

  sealed trait FieldMode extends Product with Serializable
  object FieldMode {
    case object Nullable extends FieldMode
    case object Required extends FieldMode
    case object Repeated extends FieldMode

    def get(b: Boolean): FieldMode =
      if (b) Required else Nullable

    def sort(fieldMode: FieldMode): Int =
      fieldMode match {
        case Required => -1
        case Repeated => 0
        case Nullable => 1
      }
  }
}

