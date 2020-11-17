package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import fs2.Stream
import cats.effect.Concurrent
import blobstore.Path
import blobstore.implicits.GetOps
import cats.syntax.all._
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import io.chrisdavenport.log4cats.Logger
import cats.effect.Sync

object Recover {

  val GcsPrefix = "gs://"

  def preparePath: GcsPath => Path =
    gcsPath => Path(GcsPrefix + gcsPath.bucket + "/" + gcsPath.path)

  def recoverFailedInserts[F[_]: Concurrent: Logger](resources: Resources[F]): Stream[F, Unit] =
    for {
      _    <- Stream.eval(Logger[F].info(s"Starting recovery stream."))
      _    <- Stream.eval(Logger[F].info(s"Resources for recovery: $resources"))
      path <- Stream.eval(Sync[F].pure(preparePath(resources.bucket)))
      _    <- Stream.eval(Logger[F].info(s"Path for recovery: $path"))
      r    <- recoverStream(resources, path)
    } yield r

  def recoverStream[F[_]: Concurrent: Logger](resources: Resources[F], path: Path): Stream[F, Unit] =
    resources
      .store
      .list(path)
      .evalMap(writeListingToLogs)
      .filter(keepTimePeriod)
      .evalMap(resources.store.getContents)
      .filter(keepInvalidColumnErrors)
      .map(parseJson)
      .evalMap(printEventId)
      .map(recover)
      .evalMap(writeToPubSub(resources))

  def writeToPubSub[F[_]: Concurrent: Logger](
    resources: Resources[F]
  ): Either[String, String] => F[Unit] = {
    case Left(e) => Logger[F].error(s"Error: $e")
    case Right(event) =>
      for {
        _     <- Logger[F].info(s"Preparing to write to PubSub for recovery: $event")
        msgId <- resources.pubSubProducer.produce(event)
        _     <- Logger[F].info(s"Successfully writen to PubSub: $msgId")
      } yield ()
  }

  def writeListingToLogs[F[_]: Concurrent: Logger]: Path => F[Path] =
    p =>
      for {
        _ <- Logger[F].info(s"Processing file: ${p.fileName}, path: ${p.pathFromRoot}, isDir: ${p.isDir}")
      } yield p

  def keepTimePeriod[F[_]: Concurrent: Logger]: Path => Boolean =
    p => p.fileName.map(_.startsWith("2020-11")).getOrElse(false)

  def keepInvalidColumnErrors[F[_]: Concurrent: Logger]: String => Boolean =
    _.contains("no such field.")

  case class IdAndEvent(id: String, event: String)

  def parseJson: String => Either[String, IdAndEvent] =
    failed => {
      import io.circe._, io.circe.parser._
      val doc: Json    = parse(failed).getOrElse(Json.Null)
      val payload      = doc.hcursor.downField("data").downField("payload")
      val quoted       = payload.as[String].getOrElse("")
      val quotedParsed = parse(quoted).getOrElse(Json.Null)
      val innerPayload = quotedParsed.hcursor.downField("payload").as[Json].getOrElse(Json.Null)
      val modifiedversion =
        innerPayload.hcursor.downField("event_version").withFocus(_.mapString(_ => "1-0-1")).top.getOrElse(Json.Null)
      val eventId = quotedParsed.hcursor.downField("eventId").as[String].getOrElse("")

      Right(IdAndEvent(eventId, modifiedversion.spaces2))
    }

  def printEventId[F[_]: Concurrent: Logger]: Either[String, IdAndEvent] => F[Either[String, IdAndEvent]] =
    x =>
      for {
        _ <- Logger[F].info(s"Event id for recovery: ${x.map(_.id)}")
      } yield x

  def recover: Either[String, IdAndEvent] => Either[String, String] = idAndEvent => idAndEvent.map(_.event).map(fix)

  def fix: String => String = _.replaceAll("availability_%", "availability_percentage")

}
