package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import org.joda.time.DateTime

import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref

import fs2.{ Stream, Chunk }
import scala.concurrent.duration._


import io.chrisdavenport.log4cats.Logger

import com.google.cloud.bigquery._

import com.permutive.pubsub.consumer.Model

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.services.Storage


object Flow {

  /** Time to wait since etl_tstamp to ack an event */
  val DefaultBackoffTime = 10

  /**
    * Main sink, processing data parsed from `failedInserts`
    * Attempts to insert a record into BigQuery. If insertion fails - turn it into a `Desperate`
    * and forward to a specific queue, which later will be sinked into GCS
    * @param resources all application resources
    * @param events stream of events from PubSub
    */
  def process[F[_]: Logger: Timer: ConcurrentEffect](resources: Resources[F])(events: Stream[F, Model.Record[F, EventContainer]]): Stream[F, Unit] = {
    val inserting = events
      .evalMap(checkAndInsert[F](resources.bigQuery, resources.env.config.datasetId, resources.env.config.tableId))
      .evalMap {
        case Right(_) => resources.logInserted
        case Left(d) => resources.logAbandoned *> resources.desperates.enqueue1(d)
      }
    inserting.concurrently(dequeueDesperates(resources))
  }

  object WeirdChunks {
    import cats.effect.concurrent.Ref
    import fs2.concurrent.Queue
    import scala.concurrent.duration._

    implicit val t = IO.timer(scala.concurrent.ExecutionContext.global)
    implicit val cs = IO.contextShift(scala.concurrent.ExecutionContext.global)

    def init: IO[(Ref[IO, Int], Queue[IO, String])] =
      (Ref.of[IO, Int](0), Queue.bounded[IO, String](10)).tupled

    def generate(c: Ref[IO, Int]): IO[Either[String, Int]] =
      (IO.sleep(200.millis) *> c.update(_ + 1) *> c.get).map { cc => if (cc % 2 != 0) Left(s"$cc is odd") else Right(cc) }

    // I expect that no matter how elements are inserted, they should be grouped by 10/2sec
    def pull(q: Queue[IO, String]): Stream[IO, Unit] =
      q.dequeue.groupWithin(10, 2.seconds).evalMap { c => IO(println(c.toList)) } // It always prints List with single element

    def start(i: Int): IO[Unit] = {
      val stream = for {
        r <- Stream.eval(init)
        (c, q) = r
        a <- Stream.repeatEval(generate(c)).take(i)
        putting = a match {   // Behaves differently if I flatMap it
          case Right(n) => Stream.eval(IO(println(s"$n is fine")))
          case Left(error) => Stream.eval(q.enqueue1(error))
        }
        pulling = pull(q)
        _ <- putting.concurrently(pulling)
      } yield ()

      stream.compile.drain
    }
  }

  WeirdChunks.start(50)


  /** Process dequeueing desperates and sinking them to GCS */
  def dequeueDesperates[F[_]: Timer: ConcurrentEffect: Logger](resources: Resources[F]): Stream[F, Unit] =
    resources
      .desperates
      .dequeueChunk(resources.bufferSize)
      .groupWithin(resources.bufferSize, resources.windowTime.seconds)
      .evalMap(sinkChunk(resources.counter, resources.bucket))

  /** Sink whole chunk of desperates into a filename, composed of time and chunk number */
  def sinkChunk[F[_]: Timer: ConcurrentEffect: Logger](counter: Ref[F, Int], bucket: GcsPath)(chunk: Chunk[Desperate]): F[Unit] =
    for {
      time <- getTime
      _ <- counter.update(_ + 1)
      i <- counter.get
      file = Storage.getFileName(bucket.path, i, time)
      _  <- Storage.uploadChunk[F](bucket.bucket, file)(chunk)
    } yield ()

  def checkAndInsert[F[_]: Sync: Logger](client: BigQuery,
                                         dataset: String,
                                         table: String)
                                        (event: Model.Record[F, EventContainer]): F[Either[Desperate, Unit]] =
    for {
      ready <- event.value.isReady(DefaultBackoffTime)
      result <- if (ready)
        event.ack >>
          services.Database.insert[F](client, dataset, table, event.value)
      else
        Logger[F].debug(s"Event ${event.value.eventId}/${event.value.etlTstamp} is not ready yet. Nack") >>
          event.nack.map(_.asRight)
    } yield result

  private def getTime[F[_]: Sync]: F[DateTime] =
    Sync[F].delay(DateTime.now())
}
