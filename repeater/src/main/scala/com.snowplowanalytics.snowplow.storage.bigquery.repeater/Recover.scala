package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import fs2.Stream
import cats.effect.Concurrent
import blobstore.Path
import blobstore.implicits.GetOps
import cats.effect.Sync
import cats.syntax.all._
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import io.chrisdavenport.log4cats.Logger

object Recover {

  val GcsPrefix = "gs://"

  def preparePath: GcsPath => Path =
    gcsPath => Path(GcsPrefix + gcsPath.bucket + gcsPath.path)

  def recoverFailedInserts[F[_]: Concurrent: Logger](resources: Resources[F]): Stream[F, Unit] =
    resources
      .store
      .list(preparePath(resources.bucket))
      .evalMap(writeListingToLogs)
      .filter(keepTimePeriod)
      .evalMap(resources.store.getContents)
      .filter(keepInvalidColumnErrors)
      .map(recover)
      .evalMap(writeToPubSub(resources))

  def writeToPubSub[F[_]: Concurrent: Logger](
    resources: Resources[F]
  ): Either[String, String] => F[Unit] = {
    case Left(e) => Logger[F].error(s"Error: $e")
    case Right(event) =>
      for {
        msgId <- resources.pubSubProducer.produce(event)
        _     <- Logger[F].info(s"Successfully writen to PubSub: $msgId")
      } yield ()
  }

  def writeListingToLogs[F[_]: Concurrent: Logger]: Path => F[Path] =
    p =>
      Logger[F].info(s"Processing file: ${p.fileName}, path: ${p.pathFromRoot}, isDir: ${p.isDir}") *> Sync[F].delay(p)

  def keepTimePeriod[F[_]: Concurrent: Logger]: Path => Boolean =
    p => p.fileName.map(_.startsWith("2020-11")).getOrElse(false)

  def keepInvalidColumnErrors[F[_]: Concurrent: Logger]: String => Boolean =
    _.contains("no such field.")

  def recover: String => Either[String, String] =
    failed => {
      import io.circe._, io.circe.parser._
      val doc: Json    = parse(failed).getOrElse(Json.Null)
      val payload      = doc.hcursor.downField("data").downField("payload")
      val quoted       = payload.as[String].getOrElse("")
      val innerPayload = parse(quoted).getOrElse(Json.Null).hcursor.downField("payload").as[Json].getOrElse(Json.Null)
      Right(fix(innerPayload.spaces2))
    }

  def fix: String => String = _.replaceAll("_%", "_percentage")

}
