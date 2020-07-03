//package com.snowplowanalytics.snowplow.storage.bigquery.fs2loader
//
//import fs2.{Pipe, Stream}
//import fs2.concurrent.Queue
//import cats.effect.{Concurrent, ContextShift, IO, IOApp, Timer}
//import org.specs2.mutable.Specification
//
//import scala.concurrent.duration._
//import scala.util.Random
//import Fs2Loader._
//
//class Fs2LoaderSpec extends Specification {
//
//  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
//  implicit val C: Concurrent[IO]    = IO.ioConcurrentEffect
//  implicit val T: Timer[IO]         = IO.timer(concurrent.ExecutionContext.global)
//
//  "aggregateTypes" should {
//    "produce one element per group" >> {
//      val random = new Random
//
//      val inputSizes = (1 to 50).toList
//      val groups     = inputSizes
//      val values     = (1 to 10).toList
//      val TimeWindow = (10 * 60).seconds
//
//      val inputs = for {
//        size <- inputSizes
//        l     = values.length
//        value = IO.delay(random.shuffle(values).take(random.nextInt(l)).toSet)
//      } yield Stream.eval[IO, Set[Int]](value).repeatN(size).take(size)
//
//      val outputsAndExpectations = for {
//        input          <- inputs
//        numberOfGroups <- groups
//        inputSize = input.compile.toList.flatMap(i => IO(i.length)).unsafeRunSync()
//        n         = math.ceil(inputSize.toDouble / numberOfGroups.toDouble).toInt
//        g = if (inputSize == n) {
//          1
//        } else if (n == 1) {
//          inputSize
//        } else if (inputSize % n == 0) {
//          inputSize / n
//        } else {
//          (inputSize / n) + 1
//        }
//      } yield (input.through(aggregateTypes(n, TimeWindow)).compile.toList.unsafeRunSync(), g)
//
//      forall(outputsAndExpectations.map { t =>
//        t._1.length == t._2
//      })(_ must beTrue)
//    }
//
//    "produce the correct aggregates over a number of elements" >> {
//      val input     = Stream(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7)).covary[IO].take(6)
//      val inputSize = input.compile.toList.flatMap(i => IO(i.length)).unsafeRunSync()
//      val ns        = (1 to inputSize).toList
//
//      val outputs = for {
//        n <- ns
//        output = input.through(Fs2Loader.aggregateTypes(n, 10.seconds)).compile.toList.unsafeRunSync()
//      } yield output
//
//      val expectations = List(
//        List(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7)),
//        List(Set(1, 2, 3, 4), Set(1, 4, 5), Set(2, 6, 7)),
//        List(Set(1, 2, 3, 4), Set(1, 5, 2, 6, 7)),
//        List(Set(1, 2, 3, 4, 5), Set(2, 6, 7)),
//        List(Set(1, 2, 3, 4, 5, 6), Set(2, 7)),
//        List(Set(1, 2, 3, 4, 5, 6, 7))
//      )
//
//      val outputsAndExpectations = outputs.zip(expectations)
//
//      forall(outputsAndExpectations.map { t =>
//        t._1 == t._2
//      })(_ must beTrue)
//    }
//
//    "produce the correct aggregates over a time window" >> {
//      val input1    = Stream(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7)).covary[IO].take(6)
//      val output1   = input1.through(aggregateTypes(Int.MaxValue, 1.second)).compile.toList.unsafeRunSync()
//      val expected1 = List(Set(1, 2, 3, 4, 5, 6, 7))
//
//      val input2    = (Stream(Set(1, 2)) ++ Stream.sleep_(1.seconds)).repeatN(6).take(5)
//      val output2   = input2.through(aggregateTypes(Int.MaxValue, 1.second)).compile.toList.unsafeRunSync()
//      val expected2 = List(Set(1, 2), Set(1, 2), Set(1, 2), Set(1, 2), Set(1, 2))
//
//      output1 must beEqualTo(expected1)
//      output2 must beEqualTo(expected2)
//    }
//  }
//
//  "enqueueTypes" should {
//    "correctly aggregate types and add them to the queue" >> {
//      val getTypes: String => Set[Char] = _.toCharArray.toSet
//      val GroupByN   = 2
//      val TimeWindow = 1.second
//      val QueueSize  = 3
//
//      val enqueue = for {
//        queue <- Queue.bounded[IO, Set[Char]](QueueSize)
//        input = Stream("foo", "bar", "bar", "baz", "12345", "21").covary[IO]
//        _ <- input
//          .through(enqueueTypes(queue, getTypes, GroupByN, TimeWindow, queue.enqueue))
//          .take(QueueSize)
//          .compile
//          .drain
//        result <- queue.dequeue.take(QueueSize).compile.toList
//      } yield result
//
//      val output   = enqueue.unsafeRunSync()
//      val expected = List(Set('f', 'o', 'b', 'a', 'r'), Set('b', 'a', 'r', 'z'), Set('1', '2', '3', '4', '5'))
//
//      output must beEqualTo(expected)
//    }
//  }
//
//  "dequeueTypes" should {
//    "sink all aggregate types" >> {
//      val getTypes: String => Set[Char] = _.toCharArray.toSet
//      val GroupByN                                                      = 2
//      val TimeWindow                                                    = 1.second
//      val QueueSize                                                     = 3
//      def dequeueF(types: Stream[IO, Set[Char]]): Stream[IO, Set[Char]] = types.map(set => set)
//
//      val enqueueAndDequeue = for {
//        queue <- Queue.bounded[IO, Set[Char]](QueueSize)
//        input = Stream("foo", "bar", "bar", "baz", "12345", "21").covary[IO]
//        _ <- input
//          .through(enqueueTypes(queue, getTypes, GroupByN, TimeWindow, queue.enqueue))
//          .take(QueueSize)
//          .compile
//          .drain
//        result <- dequeueTypes(None)(queue, dequeueF).take(QueueSize).compile.toList
//      } yield result
//
//      val output   = enqueueAndDequeue.unsafeRunSync()
//      val expected = List(Set('f', 'o', 'b', 'a', 'r'), Set('b', 'a', 'r', 'z'), Set('1', '2', '3', '4', '5'))
//
//      output must beEqualTo(expected)
//    }
//  }
//
//  "bigquerySink" should {
//    "correctly pipe the rows through the specified sink" >> {
//      val sink: String => IO[String] = IO.delay(_)
//      val input    = Stream("foo", "bar", "baz").covary[IO].take(3)
//      val output   = input.through(bigquerySink(sink)).compile.toList.map(_.toSet).unsafeRunSync()
//      val expected = Set("foo", "bar", "baz")
//
//      output must beEqualTo(expected)
//    }
//  }
//
//  "badSink" should {
//    "correctly pipe the bad rows through the specified sink" >> {
//      val sink: String => IO[String] = IO.delay(_)
//      val input    = Stream("foo", "bar", "baz").covary[IO].take(3)
//      val output   = input.through(badSink(None)(sink)).compile.toList.map(_.toSet).unsafeRunSync()
//      val expected = Set("foo", "bar", "baz")
//
//      output must beEqualTo(expected)
//    }
//  }
//
//  "goodSink" should {
//    "correctly enqueue types and sink rows" >> {
//      val getTypes: String => Set[Char] = _.toCharArray.toSet
//      val GroupByN   = 2
//      val TimeWindow = 1.second
//      val QueueSize  = 3
//      val sink: String => IO[String] = IO.delay(_)
//
//      val results = for {
//        queue <- Queue.bounded[IO, Set[Char]](QueueSize)
//        input = Stream("foo", "bar", "baz").covary[IO]
//        bigqueryOutput <- input
//          .through(goodSink(queue, getTypes, GroupByN, TimeWindow, queue.enqueue, sink))
//          .take(QueueSize)
//          .compile
//          .toList
//          .map(_.toSet)
//        _ <- IO.delay(println("Hello 1"))
//        _ <- input
//          .through(goodSink(queue, getTypes, GroupByN, TimeWindow, queue.enqueue, sink))
//          .take(QueueSize)
//          .compile
//          .drain
//        typesOutput <- queue.dequeue.take(QueueSize).compile.toList
//        _           <- IO.delay(println("Hello 2"))
//      } yield (bigqueryOutput, typesOutput)
//
//      val output              = results.unsafeRunSync()
//      val bigqueryExpectation = Set("foo", "bar", "baz")
//      val typesExpectation    = List(Set('f', 'o', 'b', 'a', 'r'), Set('b', 'a', 'z'))
//
//      output._1 must beEqualTo(bigqueryExpectation)
//      output._2 must beEqualTo(typesExpectation)
//    }
//  }
//}
