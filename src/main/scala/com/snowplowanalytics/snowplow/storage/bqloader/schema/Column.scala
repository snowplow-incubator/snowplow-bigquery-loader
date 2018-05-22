package com.snowplowanalytics.snowplow.storage.bqloader.schema

case class Column(name: String, dataType: DataType, mode: Mode)

object Column {
  def apply(name: String, dataType: DataType): Column =
    Column(name, dataType, Mode.Nullable)
}

