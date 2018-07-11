/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bqmutator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import cats.effect.IO
import fs2.{ Stream, Scheduler }

object Main {
  def main(args: Array[String]): Unit = {
    Config.command.parse(args) match {
      case Right(config) =>
        val itemQueue = for {
          queue        <- Listener.initQueue(1000)
          _ <- Listener.startSubscription(config, Listener(queue))
        } yield queue

        val appStream = for {
          m <- Stream.eval(Mutator.initialize(config)).map(_.fold(e => throw new RuntimeException(e), x => x))
          q <- Stream.eval(itemQueue)
          i <- q.dequeue
          _ <- Stream.eval(IO(println(s"Dequeued: $i")))
          _ <- Stream.eval(m.updateTable(i))
        } yield ()

        val ticks = Scheduler[IO](2).flatMap(_.awakeEvery[IO](5.seconds)).evalMap(d => IO { println(s"Duration $d") })
        ticks.zip(appStream).compile.drain.unsafeRunSync()

      case Left(error) =>
        System.err.println(error)
        System.exit(1)
    }
  }
}
