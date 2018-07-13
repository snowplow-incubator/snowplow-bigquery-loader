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
package com.snowplowanalytics.snowplow.storage.bqloader.mutator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import cats.effect.IO

import fs2.{ Stream, Scheduler }

object Main {
  implicit class ToStream[A](val io: IO[A]) extends AnyVal {
    def stream: Stream[IO, A] = Stream.eval[IO, A](io)
  }

  def main(args: Array[String]): Unit = {
    CommandLine.parse(args) match {
      case Right(c: CommandLine.ListenCommand) =>
        val appStream = for {
          environment <- c.getEnv.stream
          mutator <- Mutator.initialize(environment).map(_.fold(e => throw new RuntimeException(e), x => x)).stream
          queue <- TypeReceiver.initQueue(1000).stream
          _ <- TypeReceiver.startSubscription(environment.config, TypeReceiver(queue)).stream
          items <- queue.dequeue
          _ <- Stream.eval(mutator.updateTable(items))
        } yield ()

        val ticks = Scheduler[IO](2).flatMap(_.awakeEvery[IO](5.seconds))
        ticks.zip(appStream).compile.drain.unsafeRunSync()

      case Right(CommandLine.CreateCommand(config)) =>
        ???   // Port from Loader/Beam

      case Left(error) =>
        System.err.println(error)
        System.exit(1)
    }
  }
}
