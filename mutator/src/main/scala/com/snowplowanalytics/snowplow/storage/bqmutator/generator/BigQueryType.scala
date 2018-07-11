package com.snowplowanalytics.snowplow.storage.bqmutator.generator

/** Primitive BigQuery types */
sealed trait BigQueryType

object BigQueryType {
case object String extends BigQueryType

  case object Boolean extends BigQueryType

  case object Bytes extends BigQueryType

  case object Integer extends BigQueryType

  case object Float extends BigQueryType

  case object Date extends BigQueryType

  case object DateTime extends BigQueryType

}

