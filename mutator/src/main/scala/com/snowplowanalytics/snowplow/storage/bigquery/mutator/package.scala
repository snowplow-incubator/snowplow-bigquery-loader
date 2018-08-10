package com.snowplowanalytics.snowplow.storage.bigquery

import fs2.Stream

import cats.effect.IO

package object mutator {
  implicit class ToStream[A](val io: IO[A]) extends AnyVal {
    def stream: Stream[IO, A] = Stream.eval[IO, A](io)
  }
}
