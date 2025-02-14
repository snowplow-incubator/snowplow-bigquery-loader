/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import io.circe.Json

import java.time.{Instant, LocalDate}
import org.json.{JSONArray, JSONObject}

import com.snowplowanalytics.iglu.schemaddl.parquet.Type
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster

import scala.jdk.CollectionConverters._

/** Converts schema-ddl values into objects which are compatible with the snowflake ingest sdk */
private[processing] object BigQueryCaster extends Caster[AnyRef] {

  override def nullValue: Null                             = null
  override def jsonValue(v: Json): String                  = v.noSpaces
  override def stringValue(v: String): String              = v
  override def booleanValue(v: Boolean): java.lang.Boolean = Boolean.box(v)
  override def intValue(v: Int): java.lang.Integer         = Int.box(v)
  override def longValue(v: Long): java.lang.Long          = Long.box(v)
  override def doubleValue(v: Double): java.lang.Double    = Double.box(v)
  override def decimalValue(unscaled: BigInt, details: Type.Decimal): java.math.BigDecimal =
    new java.math.BigDecimal(unscaled.bigInteger, details.scale)
  override def timestampValue(v: Instant): java.lang.Long = Long.box(v.toEpochMilli * 1000) // Microseconds
  override def dateValue(v: LocalDate): java.lang.Long    = Long.box(v.toEpochDay)
  override def arrayValue(vs: List[AnyRef]): JSONArray =
    // BigQuery does not permit nulls in a repeated field
    new JSONArray(vs.filterNot(_ == null).asJava)
  override def structValue(vs: List[Caster.NamedValue[AnyRef]]): JSONObject = {
    val map = vs
      .map { case Caster.NamedValue(k, v) =>
        (k, v)
      }
      .toMap
      .asJava
    new JSONObject(map)
  }
}
