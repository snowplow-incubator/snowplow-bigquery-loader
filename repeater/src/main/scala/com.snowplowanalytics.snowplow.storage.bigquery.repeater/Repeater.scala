/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import cats.Applicative
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref

import org.http4s.client.Client
import org.http4s.client.asynchttpclient.AsyncHttpClient

import fs2.Stream
import fs2.concurrent.{ Queue, Signal, SignallingRef }

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.google.cloud.bigquery._

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.http.{PubsubHttpConsumer, PubsubHttpConsumerConfig}

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate

import scala.util.control.NonFatal

object Repeater extends IOApp {

  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val DefaultBufferSize = 5
  val DefaultBackoffTime = 5

  def getServiceAccountPath[F[_]: Sync]: F[String] =
    Sync[F].delay(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

  def getSubscription[F[_]: Concurrent: Timer: Logger](env: Config.Environment,
                                                       serviceAccount: String,
                                                       subscription: String,
                                                       client: Client[F]) = {
    PubsubHttpConsumer.subscribe[F, EventContainer](
      Model.ProjectId(env.config.projectId),
      Model.Subscription(subscription),
      serviceAccount,
      PubsubHttpConsumerConfig[F](
        host = "pubsub.googleapis.com",
        readReturnImmediately = true,
        port = 443,
        readConcurrency = 2
      ),
      client,
      (msg, err, ack, _) => Logger[F].error(s"Msg $msg got error $err") >> ack,
    )
  }

  def getClient[F[_]: Sync]: F[BigQuery] =
    Sync[F].delay(BigQueryOptions.getDefaultInstance.getService)

  def buildRequest(dataset: String, table: String, event: EventContainer) =
    InsertAllRequest.newBuilder(TableId.of(dataset, table))
      .addRow(event.eventId.toString, event.decompose)
      .build()

  def insert[F[_]: Sync](client: BigQuery, dataset: String, table: String, event: EventContainer) = {
    val request = buildRequest(dataset, table, event)
    Sync[F].delay(client.insertAll(request)).attempt.map {
      case Right(response) =>
        if (response.hasErrors)
          Left(EventContainer.Desperate(event.payload, EventContainer.FailedRetry.extract(response.getInsertErrors)))
        else
          Right(())
      case Left(throwable: BigQueryException) =>
        Desperate(event.payload, EventContainer.FailedRetry.extract(throwable)).asLeft
      case Left(unknown) =>
        throw unknown
    }
  }

  def process[F[_]: Sync: Logger](client: BigQuery,
                                  dataset: String,
                                  table: String,
                                  event: Model.Record[F, EventContainer]): F[Either[Desperate, Unit]] =
    for {
      ready <- event.value.isReady(DefaultBackoffTime)
      result <- if (ready) event.ack >> insert[F](client, dataset, table, event.value) else event.nack.map(_.asRight)
    } yield result

  def sinkDesperate[F[_]: ConcurrentEffect: Logger](path: GcsPath,
                                                    buffer: Queue[F, Desperate],
                                                    counter: Ref[F, Int],
                                                    element: Desperate): F[Unit] =
    for {
      added <- buffer.offer1(element)
      _ <- if (added) Applicative[F].unit else for {
        time <- Storage.getTime
        _ <- counter.update(_ + 1)
        i <- counter.get
        file = Storage.getFileName(path.path, i, time)
        _ <- buffer
          .dequeue
          .through(_.take(DefaultBufferSize))
          .through(Storage.uploadFile[F](path.bucket, file))
          .compile
          .drain
      } yield ()
    } yield ()

  // TODO: use it
  def flush[F[_]: Sync](buffer: Queue[F, Desperate]): F[List[Desperate]] = {
    val last = Stream
      .repeatEval(buffer.tryDequeueChunk1(10))
      .takeWhile(_.isDefined)
      .flatMap {
        case Some(chunk) => Stream.chunk(chunk)
        case None => Stream.empty
      }
    last.compile.toList
  }

  /**
    * Resources container, allowing to manipulate all acquired entities
    * @param bigQuery BigQuery client provided by SDK
    * @param bucket destination path
    * @param env whole configuration, extracted from config files
    * @param queue
    * @param counter
    * @param stop
    * @param serviceAccount
    * @param client
    */
  class Env[F[_]](val bigQuery: BigQuery,
                  val bucket: GcsPath,
                  val env: Config.Environment,
                  val queue: Queue[F, Desperate],
                  val counter: Ref[F, Int],
                  val stop: Signal[F, Boolean],
                  val serviceAccount: String,
                  val client: Client[F])

  object Env {
    def acquire[F[_]: ConcurrentEffect: Clock](command: RepeaterCli.ListenCommand): Resource[F, Env[F]] = {
      val environment = for {
        transformed <- Config.transform[F](command.config).value
        env <- Sync[F].fromEither(transformed)
        bigQuery <- getClient[F]
        serviceAccount <- getServiceAccountPath[F]
        queue      <- Queue.bounded[F, Desperate](DefaultBufferSize)
        counter    <- Ref[F].of[Int](0)
        stop <- SignallingRef[F, Boolean](false)
      } yield new Env(bigQuery, command.deadEndBucket, env, queue, counter, stop, serviceAccount, _: Client[F])


      for {   // Initialize resource
        envF <- Resource.liftF(environment)
        client <- AsyncHttpClient.resource[F]()
      } yield envF(client)
    }
  }

  def sink[F[_]: Logger: ConcurrentEffect](env: Env[F])(stream: Stream[F, Model.Record[F, EventContainer]]) = {
    for {
      event      <- stream
      act         = process[F](env.bigQuery, env.env.config.datasetId, env.env.config.tableId, event)
      insertion  <- Stream.eval(act)
      desperate  <- insertion match {
        case Right(_) => Stream.empty
        case Left(e) => Stream.emit(e)
      }
      _ <- Stream.eval(sinkDesperate(env.bucket, env.queue, env.counter, desperate))
    } yield ()
  }

  def run(args: List[String]): IO[ExitCode] = {
    RepeaterCli.parse(args) match {
      case Right(command) =>
        val events = for {
          env   <- Stream.resource(Env.acquire[IO](command))
          events = getSubscription(env.env, env.serviceAccount, command.failedInsertsSub, env.client).interruptWhen(env.stop)
          _     <- sink(env)(events)
        } yield ()

        events
          .compile
          .drain
          .attempt
          .flatMap {
            case Right(_) =>
              IO.delay(println("Closing Snowplow BigQuery Repeater")) *> IO.pure(ExitCode.Success)
            case Left(Config.InitializationError(message)) =>
              IO(System.err.println(s"Snowplow BigQuery Repeater failed to start. $message")) >> IO.pure(ExitCode.Error)
            case Left(e: java.util.concurrent.TimeoutException) =>
              println(e.getCause)
              throw e
            case Left(NonFatal(e)) =>
              IO.raiseError(e) >> IO.pure(ExitCode.Error)
          }
      case Left(error) =>
        IO(println(error.toString())) >> IO.pure(ExitCode.Error)
    }
  }
}
