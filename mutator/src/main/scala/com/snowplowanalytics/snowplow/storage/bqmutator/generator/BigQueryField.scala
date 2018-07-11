package com.snowplowanalytics.snowplow.storage.bqmutator.generator

import scala.collection.immutable.ListMap

/** Type-safe isomorphic proxy to `Field` */
sealed trait BigQueryField {
  def mode: FieldMode

  def setMode(mode: FieldMode): BigQueryField = this match {
    case BigQueryField.Primitive(t, _) => BigQueryField.Primitive(t, mode)
    case BigQueryField.Record(_, fields) => BigQueryField.Record(mode, fields)
    case array => array
  }
}

object BigQueryField {

  case class Primitive(bigQueryType: BigQueryType, mode: FieldMode) extends BigQueryField

  case class Record(mode: FieldMode, subFields: ListMap[String, BigQueryField]) extends BigQueryField

  case class Array(field: BigQueryField) extends BigQueryField {
    def mode: FieldMode = FieldMode.Repeated
  }
}

