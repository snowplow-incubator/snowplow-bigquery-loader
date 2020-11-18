package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.util.UUID

import scala.concurrent.duration._

import fs2.{Pipe, Stream}

import blobstore.Path
import blobstore.implicits.GetOps

import cats.Monad
import cats.implicits._
import cats.effect.{Concurrent, Timer}

import io.chrisdavenport.log4cats.Logger

import io.circe._
import io.circe.parser._
import io.circe.syntax._

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.services.Storage

object Recover {

  val ConcurrencyLevel = 64

  val GcsPrefix = "gs://"

  def preparePath: GcsPath => Path =
    gcsPath => Path(GcsPrefix + gcsPath.bucket + "/" + gcsPath.path)

  def recoverFailedInserts[F[_]: Concurrent: Logger: Timer](resources: Resources[F]): Stream[F, Unit] =
    for {
      _    <- Stream.eval(Logger[F].info(s"Starting recovery stream."))
      _    <- Stream.eval(Logger[F].info(s"Resources for recovery: $resources"))
      path  = preparePath(resources.bucket)
      _    <- Stream.eval(Logger[F].info(s"Path for recovery: $path"))
      _    <- recoverStream(resources, path)
    } yield ()

  def recoverStream[F[_]: Concurrent: Timer: Logger](resources: Resources[F], path: Path): Stream[F, Unit] =
    resources
      .store
      .list(path)
      .evalMap(writeListingToLogs[F])
      .filter(keepTimePeriod)
      .evalMap(resources.store.getContents)
      .through(fs2.text.lines)
      .evalTap(_ => resources.logTotal)
      .filter(keepInvalidColumnErrors)
      .map(recover(resources.problematicContext, resources.fixedContext))
      .observeEither(badPipe(resources), goodPipe(resources))
      .void

  def goodPipe[F[_]: Logger: Concurrent](resources: Resources[F]): Pipe[F, IdAndEvent, Unit] =
    _.parEvalMapUnordered(ConcurrencyLevel) { event =>
      resources.pubSubProducer.produce(event.event.noSpaces).flatMap { msgId =>
        Logger[F].info(s"Written ${event.id}: $msgId")
      } *> resources.logRecovered
    }

  def badPipe[F[_]: Logger: Concurrent: Timer](resources: Resources[F]): Pipe[F, FailedRecovery, Unit] =
    in => in
      .evalTap(_ => resources.logFailed)
      .groupWithin(resources.bufferSize, resources.windowTime.seconds)
      .parEvalMapUnordered(ConcurrencyLevel) { chunk =>
        for {
          _    <- resources.counter.update(_ + 1)
          i    <- resources.counter.get
          file = Storage.getFileName(resources.bucket.path + s"recovery-${generated.BuildInfo.version}/", i)
          _ <- services.Storage.uploadJson(resources.bucket.bucket, file, chunk.map(_.asJson))
        } yield ()
      }

  case class IdAndEvent(id: UUID, event: Json)

  case class FailedRecovery(original: String, error: Error)

  implicit val failedRecoveryEncoder: Encoder[FailedRecovery] =
    Encoder.instance { recovery =>
      Json.fromFields(List(
        "original" -> recovery.original.asJson,
        "error" -> recovery.error.show.asJson
      ))
    }

  /** Try to parse loader_recovery_error bad row and fix it, attaching event id */
  def recover(columnToFix: String, fixedColumn: String)(failed: String): Either[FailedRecovery, IdAndEvent] = {
    val result = for {
      doc          <- parse(failed)
      payload       = doc.hcursor.downField("data").downField("payload")
      quoted       <- payload.as[String]
      quotedParsed <- parse(quoted)
      innerPayload <- quotedParsed.hcursor.downField("payload").as[Json]
      eventId      <- quotedParsed.hcursor.downField("eventId").as[UUID]

      fixedPayload <- fix(innerPayload, columnToFix, fixedColumn)

    } yield IdAndEvent(eventId, fixedPayload)

    result.leftMap { error => FailedRecovery(failed, error) }
  }

  /** Fix `payload` property from loader_recovery_error bad row
    * by replacing "availability_%" with "availability_percentage" keys
    * in `ColumnToFix` column
    */
  def fix(payload: Json, columnToFix: String, fixedColumn: String): Either[DecodingFailure, Json] =
    for {
      jsonObject <- payload.as[JsonObject].map(_.toMap)
      fixed = jsonObject.map {
        case (key, value) if key == columnToFix && value.isArray =>
          val fixedContexts = value.asArray.getOrElse(Vector.empty).map { context =>
            val fixedContext = context.asObject.map { hash =>
              val fixedHash = hash.toMap.map {
                case ("availability_%", value) => ("availability_percentage", value)
                case (key, value) => (key, value)
              }
              Json.fromFields(fixedHash)
            }
            fixedContext.getOrElse(context)
          }
          (fixedColumn, Json.fromValues(fixedContexts))
        case (key, value) => (key, value)
      }
    } yield Json.fromFields(fixed)

  def writeListingToLogs[F[_]: Monad: Logger](p: Path): F[Path] =
    Logger[F].info(s"Processing file: ${p.fileName}, path: ${p.pathFromRoot}, isDir: ${p.isDir}").as(p)

  def keepTimePeriod(p: Path): Boolean =
    p.fileName.exists(_.startsWith("2020-11"))

  def keepInvalidColumnErrors(f: String): Boolean =
    f.contains("no such field.")
}
