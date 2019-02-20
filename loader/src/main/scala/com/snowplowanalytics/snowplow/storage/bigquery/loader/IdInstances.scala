package com.snowplowanalytics.snowplow.storage.bigquery.loader

import java.util.concurrent.TimeUnit

import cats.Id
import cats.effect.Clock

object IdInstances {
  implicit val idClock: Clock[Id] = new Clock[Id] {
    def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

    def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }
}
