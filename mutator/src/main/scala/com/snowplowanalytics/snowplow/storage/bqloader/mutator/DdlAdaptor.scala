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

import com.google.cloud.bigquery.{Field, FieldList, LegacySQLTypeName}

import com.snowplowanalytics.iglu.schemaddl.bigquery.Field._
import com.snowplowanalytics.iglu.schemaddl.bigquery.Generator._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field => BigQueryField, _}

import scala.collection.convert.decorateAsJava._


/** Transform dependency-free Schema DDL AST into Google Cloud Java definitions */
object DdlAdaptor {

  def fromColumn(column: Column): Field =
    adaptField(column.bigQueryField)
      .toBuilder
      .setDescription(column.version.toSchemaUri)
      .build()

  def adaptField(bigQueryField: BigQueryField): Field =
    bigQueryField match {
      case BigQueryField(name, r @ BigQueryType.Record(fields), m) =>
        val subFields = fields.map(adaptField)
        val fieldsList = FieldList.of(subFields.asJava)
        Field.newBuilder(name, adaptType(r), fieldsList).setMode(adaptMode(m)).build()
      case BigQueryField(name, t, m @ FieldMode.Repeated) =>
        adaptField(BigQueryField(name, t, m))
      case BigQueryField(name, t, m) =>
        Field.newBuilder(name, adaptType(t)).setMode(adaptMode(m)).build()
    }

  def adaptMode(fieldMode: FieldMode): Field.Mode =
    fieldMode match {
      case FieldMode.Nullable => Field.Mode.NULLABLE
      case FieldMode.Required => Field.Mode.REQUIRED
      case FieldMode.Repeated => Field.Mode.REPEATED
    }

  def adaptType(fieldType: BigQueryType): LegacySQLTypeName =
    fieldType match {
      case BigQueryType.DateTime => LegacySQLTypeName.DATETIME
      case BigQueryType.Integer => LegacySQLTypeName.INTEGER
      case BigQueryType.Boolean => LegacySQLTypeName.BOOLEAN
      case BigQueryType.String => LegacySQLTypeName.STRING
      case BigQueryType.Float => LegacySQLTypeName.FLOAT
      case BigQueryType.Bytes => LegacySQLTypeName.BYTES
      case BigQueryType.Date => LegacySQLTypeName.DATE
      case BigQueryType.Record(_) => LegacySQLTypeName.RECORD
    }
}
