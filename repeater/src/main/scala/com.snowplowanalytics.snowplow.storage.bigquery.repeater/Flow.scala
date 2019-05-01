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
  def process[F[_]: Logger: Timer: ConcurrentEffect](resources: Resources[F])(events: Stream[F, Model.Record[F, EventContainer]]): Stream[F, Unit] =
    for {
      event      <- events
      act         = checkAndInsert[F](resources.bigQuery, resources.env.config.datasetId, resources.env.config.tableId, event)
      insertion  <- Stream.eval(act)
      desperateSink = insertion match {
        case Right(_) => Stream.eval(resources.logInserted)
        case Left(d) => Stream.eval(resources.logAbandoned *> resources.desperates.enqueue1(d))
      }
      _ <- desperateSink.concurrently(dequeueDesperates(resources))
    } yield ()

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
                                         table: String,
                                         event: Model.Record[F, EventContainer]): F[Either[Desperate, Unit]] =
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
