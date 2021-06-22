/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.{ShreddedType, UnstructEvent}
import org.specs2.mutable.Specification
import fs2.Stream
import scala.util.Random
import scala.concurrent.duration._

class StreamLoaderSpec extends Specification {

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)
  implicit val C: Concurrent[IO]    = IO.ioConcurrentEffect
  implicit val T: Timer[IO]         = IO.timer(concurrent.ExecutionContext.global)

  private def mkShreddedType(n: Int): ShreddedType = {
    val schemaKey = SchemaKey("com.acme", "example", "jsonschema", SchemaVer.Full(1, 0, n))
    ShreddedType(UnstructEvent, schemaKey)
  }

  private def mkShreddedTypes(seeds: List[Set[Int]]): List[Set[ShreddedType]] =
    seeds.map(set => set.map(mkShreddedType))

  "aggregateTypes" should {
    import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.StreamLoader.aggregateTypes

    "produce one element per group" in {
      val random = new Random

      val inputSizes = (1 to 50).toList
      val groups     = inputSizes
      val seeds      = (1 to 10).toList
      val values     = seeds.map(mkShreddedType)
      val TimeWindow = (10 * 60).seconds

      val inputs = for {
        size <- inputSizes
        l     = values.length
        value = IO.delay(random.shuffle(values).take(random.nextInt(l)).toSet)
      } yield Stream.eval[IO, Set[ShreddedType]](value).repeatN(size.toLong).take(size.toLong)

      val outputsAndExpectations = for {
        input          <- inputs
        numberOfGroups <- groups
        inputSize = input.compile.toList.flatMap(i => IO(i.length)).unsafeRunSync()
        n         = math.ceil(inputSize.toDouble / numberOfGroups.toDouble).toInt
        g = if (inputSize == n) {
          1
        } else if (n == 1) {
          inputSize
        } else if (inputSize % n == 0) {
          inputSize / n
        } else {
          (inputSize / n) + 1
        }
      } yield (input.through(aggregateTypes(n, TimeWindow)).compile.toList.unsafeRunSync(), g)

      forall(outputsAndExpectations.map { t =>
        t._1.length == t._2
      })(_ must beTrue)
    }

    "produce the correct aggregates over a number of elements" in {
      val seeds     = List(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7))
      val values    = mkShreddedTypes(seeds)
      val input     = Stream.emits(values).covary[IO].take(6)
      val inputSize = input.compile.toList.flatMap(i => IO(i.length)).unsafeRunSync()
      val ns        = (1 to inputSize).toList

      val outputs = for {
        n <- ns
        output = input.through(aggregateTypes(n, 10.seconds)).compile.toList.unsafeRunSync()
      } yield output

      val expectations = List(
        List(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7)),
        List(Set(1, 2, 3, 4), Set(1, 4, 5), Set(2, 6, 7)),
        List(Set(1, 2, 3, 4), Set(1, 5, 2, 6, 7)),
        List(Set(1, 2, 3, 4, 5), Set(2, 6, 7)),
        List(Set(1, 2, 3, 4, 5, 6), Set(2, 7)),
        List(Set(1, 2, 3, 4, 5, 6, 7))
      ).map(mkShreddedTypes)

      val outputsAndExpectations = outputs.zip(expectations)

      forall(outputsAndExpectations.map { t =>
        t._1 == t._2
      })(_ must beTrue)
    }

    "produce the correct aggregates over a time window" in {
      val seeds  = List(Set(1, 2), Set(3, 4), Set(1, 4), Set(1, 5), Set(2, 6), Set(2, 7))
      val values = mkShreddedTypes(seeds)

      val input1    = Stream.emits(values).covary[IO].take(6)
      val output1   = input1.through(aggregateTypes(Int.MaxValue, 1.second)).compile.toList.unsafeRunSync()
      val expected1 = mkShreddedTypes(List(Set(1, 2, 3, 4, 5, 6, 7)))

      val input2    = (Stream.emits(mkShreddedTypes(List(Set(1, 2)))) ++ Stream.sleep_(1.seconds)).repeatN(6).take(5)
      val output2   = input2.through(aggregateTypes(Int.MaxValue, 1.second)).compile.toList.unsafeRunSync()
      val expected2 = mkShreddedTypes(List(Set(1, 2), Set(1, 2), Set(1, 2), Set(1, 2), Set(1, 2)))

      output1 must beEqualTo(expected1)
      output2 must beEqualTo(expected2)
    }
  }
}
