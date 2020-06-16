package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader

import fs2.Stream
import fs2.concurrent.Queue
import cats.effect.{Concurrent, ContextShift, IO, IOApp}
import org.specs2.mutable.Specification


class Fs2LoaderSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val C: Concurrent[IO] = IO.ioConcurrentEffect

  "aggregateTypes" should {
    "aggreagate objects" >> {
      val input = Stream.emits(List(Set(1,2), Set(3,4), Set(1,4), Set(1,5))).covary[IO]
      val output = Fs2Loader.aggregateTypes(input).take(1).compile.toList.unsafeRunSync()
      val expected = List(Set(1,2,3,4,5))
      output must beEqualTo(expected)
    }
  }

  "enqueueTypes" should {
    "enqeue types" >> {
      val f: String => Set[Char] = _.toCharArray.toSet
      val action = for {
        queue <- Queue.bounded[IO, Set[Char]](10)
        input = Stream("foo", "bar", "bar").covary[IO]
        _ <- Fs2Loader.enqueueTypes(input, queue, f).compile.drain
        result <- queue.dequeue.take(2).compile.toList
      } yield result
      val expected = List(Set('f', 'o', 'b'))
      val output = action.unsafeRunSync()

      output must beEqualTo(expected)
    }
  }

}
