package com.snowplowanalytics.snowplow.storage.bqloader.schema

import com.snowplowanalytics.snowplow.storage.bqloader.ToDdl

sealed trait DataType

object DataType {
  case object Integer extends DataType
  case object FloatingPoint extends DataType
  case object Boolean extends DataType
  case object String extends DataType
  case object Bytes extends DataType
  case object Date extends DataType
  case object Datetime extends DataType
  case object Time extends DataType
  case object Timestamp extends DataType
  case object Struct extends DataType

  implicit object DataTypeDdl extends ToDdl[DataType] {
    override def toDdl(dt: DataType): String = dt match {
      case DataType.Integer => "INT64"
      case DataType.FloatingPoint => "FLOAT64"
      case DataType.Boolean => "BOOL"
      case DataType.String => "STRING"
      case DataType.Bytes => "BYTES"
      case DataType.Date => "DATE"
      case DataType.Datetime => "DATETIME"
      case DataType.Time => "TIME"
      case DataType.Timestamp => "TIMESTAMP"
      case DataType.Struct => "STRUCT"
    }
  }
}

