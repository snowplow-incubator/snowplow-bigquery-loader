package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.iglu.schemaddl.bigquery.Field

case class FieldValue(columnName: ColumnName, field: Field)
