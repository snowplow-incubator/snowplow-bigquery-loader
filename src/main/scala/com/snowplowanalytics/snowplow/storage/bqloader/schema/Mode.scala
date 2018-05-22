package com.snowplowanalytics.snowplow.storage.bqloader.schema

import com.snowplowanalytics.snowplow.storage.bqloader.ToDdl

sealed trait Mode

object Mode {
  case object Required extends Mode
  case object Nullable extends Mode
  case object Repeated extends Mode

  implicit object ModeDdl extends ToDdl[Mode] {
    def toDdl(ddl: Mode): String = ddl match {
      case Mode.Required => "REQUIRED"
      case Mode.Nullable => "NULLABLE"
      case Mode.Repeated => "REPEATED"
    }
  }
}

