package com.snowplowanalytics.snowplow.storage.bqmutator.generator

import scala.collection.immutable.ListMap

/** Type-safe isomorphic proxy to `Field` */
sealed trait BigQueryField {
  def mode: FieldMode
}

object BigQueryField {

  case class Primitive(bigQueryType: BigQueryType, mode: FieldMode) extends BigQueryField

  case class Record(mode: FieldMode, subFields: ListMap[String, BigQueryField]) extends BigQueryField

  case class Array(field: BigQueryField) extends BigQueryField {
    def mode: FieldMode = FieldMode.Repeated
  }
}

