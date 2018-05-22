package com.snowplowanalytics.snowplow.storage.bqloader

trait ToDdl[A] {
  def toDdl(ddl: A): String
}
