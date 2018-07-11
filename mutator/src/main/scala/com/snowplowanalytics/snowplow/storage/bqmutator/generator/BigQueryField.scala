package com.snowplowanalytics.snowplow.storage.bqmutator.generator

/** Type-safe isomorphic proxy to `Field` */
case class BigQueryField(bigQueryType: BigQueryType, mode: FieldMode) {
  def setMode(mode: FieldMode): BigQueryField = this match {
    case BigQueryField(t, _) => BigQueryField(t, mode)
  }
}

