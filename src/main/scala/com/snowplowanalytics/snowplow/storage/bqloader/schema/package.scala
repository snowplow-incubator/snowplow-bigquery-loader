package com.snowplowanalytics.snowplow.storage.bqloader

package object schema {
  implicit class ToDdlSyntax[A: ToDdl](a: A) {
    def toDdl: String = implicitly[ToDdl[A]].toDdl(a)
  }
}
