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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.storage.bigquery.mutator.MutatorCli.MutatorCommand

import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  private val MaxConcurrency = 4

  def sink(mutator: Mutator): fs2.Pipe[IO, List[ShreddedType], Unit] =
    _.parEvalMap(MaxConcurrency)(items => mutator.updateTable(items))

  def run(args: List[String]): IO[ExitCode] =
    MutatorCli.parse(args) match {
      case Right(c: MutatorCommand.Listen) =>
        val appStream = for {
          env     <- IO.delay(c.env).stream
          mutator <- Mutator.initialize(env, c.verbose).map(_.fold(e => throw new RuntimeException(e), identity)).stream
          queue   <- TypeReceiver.initQueue(512).stream
          _       <- TypeReceiver.startSubscription(env, TypeReceiver(queue, c.verbose)).stream
          _       <- IO(println(s"Mutator is listening ${env.config.input.subscription} PubSub subscription")).stream
          _       <- queue.dequeue.through(sink(mutator))
        } yield ()

        appStream.compile.drain.as(ExitCode.Success)

      case Right(c: MutatorCommand.Create) =>
        for {
          client <- TableReference.BigQueryTable.getClient
          _      <- TableReference.BigQueryTable.create(c, client)
        } yield ExitCode.Success

      case Right(c: MutatorCommand.AddColumn) =>
        for {
          env     <- IO.delay(c.env)
          mutator <- Mutator.initialize(env, true).map(_.fold(e => throw new RuntimeException(e), identity))
          _       <- mutator.addField(ShreddedType(c.property, c.schema))
        } yield ExitCode.Success

      case Left(help) =>
        IO(System.err.println(help.toString)).as(ExitCode.Error)
    }
}
