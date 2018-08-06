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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO

import fs2.Stream

object Main {
  implicit class ToStream[A](val io: IO[A]) extends AnyVal {
    def stream: Stream[IO, A] = Stream.eval[IO, A](io)
  }

  def main(args: Array[String]): Unit = {
    CommandLine.parse(args) match {
      case Right(c: CommandLine.ListenCommand) =>
        val appStream = for {
          env     <- c.getEnv.stream
          mutator <- Mutator.initialize(env).map(_.fold(e => throw new RuntimeException(e), x => x)).stream
          queue   <- TypeReceiver.initQueue(1000).stream
          _       <- TypeReceiver.startSubscription(env.config, TypeReceiver(queue)).stream
          _       <- IO(println(s"Mutator is listening ${env.config.typesSubscription}")).stream
          items   <- queue.dequeue
          _       <- mutator.updateTable(items).stream
        } yield ()

        appStream.compile.drain.unsafeRunSync()

      case Right(c: CommandLine.CreateCommand) =>
        val app = for {
          env    <- c.getEnv
          client <- TableReference.BigQueryTable.getClient
          _      <- TableReference.BigQueryTable.create(client, env.config.datasetId, env.config.tableId)
        } yield ()
        app.unsafeRunSync()

      case Left(error) =>
        System.err.println(error)
        System.exit(1)
    }
  }
}
