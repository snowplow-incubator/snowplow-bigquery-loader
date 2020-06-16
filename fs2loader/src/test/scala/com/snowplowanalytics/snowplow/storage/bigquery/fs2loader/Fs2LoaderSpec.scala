package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader

import fs2.Stream
import fs2.concurrent.Queue
import cats.effect.{Concurrent, ContextShift, IO, IOApp, Timer}
import org.specs2.mutable.Specification
import concurrent.duration._


class Fs2LoaderSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val C: Concurrent[IO] = IO.ioConcurrentEffect
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  "aggregateTypes" should {
    "aggreagate objects" >> {
      val input = Stream(Set(1,2), Set(3,4), Set(1,4), Set(1,5)).covary[IO]
      val output = input.through(Fs2Loader.aggregateTypes).take(3).compile.toList.unsafeRunSync()
      val expected = List(Set(1,2,3,4), Set(1,5))
      output must beEqualTo(expected)
    }

    "aggregate objects by time" >> {
      val input = Stream(Set(1,2)) ++ Stream.sleep_(1.seconds) ++ Stream(Set(1,2))
      val output = input.through(Fs2Loader.aggregateTypes).take(3).compile.toList.unsafeRunSync()
      val expected = List(Set(1,2))
      output must beEqualTo(expected)
    }
  }

  "enqueueTypes" should {
    "enqeue types" >> {
      val f: String => Set[Char] = _.toCharArray.toSet
      val Size = 3
      val action = for {
        queue <- Queue.bounded[IO, Set[Char]](Size)
        input = Stream("foo", "bar", "bar", "baz", "12345", "21").covary[IO]
        _ <- input.through(Fs2Loader.enqueueTypes(queue, f)).take(Size).compile.drain
        _ <- IO(println("Hello"))
        result <- queue.dequeue.take(2).compile.toList
      } yield result

      val expected = List(Set('f', 'a', 'b', 'r', 'o'), Set('f', 'a', 'b', 'r', 'o'))
      val output = action.unsafeRunSync()

      output must beEqualTo(expected)
    }
  }

}
