package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import fs2.Stream
import cats.effect.Concurrent
import cats.effect.Timer
import blobstore.Path
import blobstore.implicits.GetOps
import io.circe.JsonObject
import java.time.Instant
import java.util.UUID
import io.circe.generic.auto._, io.circe.parser._
import cats.effect.Sync
import cats.syntax.all._

object Recover {

  //TODO: hardcode proper GCS path
  val DeadQueuePath = Path("gs://sp-storage-loader-failed-inserts-dev1-com_snplow_eng_gcp/dead_queue")

  def recoverFailedInserts[F[_]: Timer: Concurrent](resources: Resources[F]): Stream[F, Unit] =
    resources
      .store
      .list(DeadQueuePath)
      .evalMap(resources.store.getContents)
      .map(recover)
      .evalMap {
        case Left(e)     => Sync[F].pure(println(s"Error: $e")) // TODO: use logger
        case Right(read) => resources.pubSubProducer.produce(read) *> Sync[F].unit
      }

  case class SimpleEventContainer(eventId: UUID, etlTstamp: Instant, payload: String)

  // TODO: filter only time period that is needed (based on names in GCS bucket)
  // TODO: filter only failures that are due to invalid column
  // TODO: count successfuly recovered events (?)
  def recover: String => Either[String, EventContainer] =
    b =>
      stringToFailedInsertBadRow(b).map { ev =>
        val fixed = fix(ev.payload)
        EventContainer(ev.eventId, ev.etlTstamp, stringToJson(fixed))
      }

  case class FailedInsert(schema: String, data: Data)
  case class Data(payload: String)

  case class Payload(eventId: UUID, etlTstamp: Instant)

  case class Combined(eventId: UUID, etlTstamp: Instant, payload: String)

  def stringToFailedInsertBadRow: String => Either[String, Combined] =
    in => {
      val parse =
        for {
          raw   <- decode[FailedInsert](in).map(_.data.payload)
          extra <- decode[Payload](raw)
        } yield Combined(extra.eventId, extra.etlTstamp, raw)

      parse.left.map(_.toString)
    }

  def fix: String => String = _.replaceAll("_%", "_percentage")

  def stringToJson: String => JsonObject = ???

}
