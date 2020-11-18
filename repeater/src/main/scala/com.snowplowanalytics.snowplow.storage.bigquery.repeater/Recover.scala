package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.util.UUID

import fs2.Stream

import blobstore.Path
import blobstore.implicits.GetOps

import cats.Monad
import cats.implicits._
import cats.effect.Concurrent

import io.chrisdavenport.log4cats.Logger

import io.circe._
import io.circe.parser._

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.RepeaterCli.GcsPath

object Recover {

  val ConcurrencyLevel = 64

  val GcsPrefix = "gs://"

  def preparePath: GcsPath => Path =
    gcsPath => Path(GcsPrefix + gcsPath.bucket + "/" + gcsPath.path)

  def recoverFailedInserts[F[_]: Concurrent: Logger](resources: Resources[F]): Stream[F, Unit] =
    for {
      _    <- Stream.eval(Logger[F].info(s"Starting recovery stream."))
      _    <- Stream.eval(Logger[F].info(s"Resources for recovery: $resources"))
      path  = preparePath(resources.bucket)
      _    <- Stream.eval(Logger[F].info(s"Path for recovery: $path"))
      r    <- recoverStream(resources, path)
    } yield r

  def recoverStream[F[_]: Concurrent: Logger](resources: Resources[F], path: Path): Stream[F, Unit] =
    resources
      .store
      .list(path)
      .evalMap(writeListingToLogs[F])
      .filter(keepTimePeriod)
      .evalMap(resources.store.getContents)
      .filter(keepInvalidColumnErrors)
      .map(recover(resources.problematicContext, resources.fixedContext))
      .evalMap(printEventId[F, Error])
      .parEvalMapUnordered(ConcurrencyLevel)(writeToPubSub(resources))

  def writeToPubSub[F[_]: Concurrent: Logger](
    resources: Resources[F]
  ): Either[Error, Json] => F[Unit] = {
    case Left(e) => Logger[F].error(e.show)
    case Right(event) =>
      Logger[F].info(s"Preparing to write to PubSub for recovery: $event") *>
        resources.pubSubProducer.produce(event.noSpaces).flatMap { msgId =>
          Logger[F].debug(s"Successfully written to PubSub: $msgId")
        }
  }

  case class IdAndEvent(id: UUID, event: Json)

  /** Try to parse loader_recovery_error bad row and fix it, attaching event id */
  def recover(columnToFix: String, fixedColumn: String)(failed: String): Either[Error, IdAndEvent] =
    for {
      doc          <- parse(failed)
      payload       = doc.hcursor.downField("data").downField("payload")
      quoted       <- payload.as[String]
      quotedParsed <- parse(quoted)
      innerPayload <- quotedParsed.hcursor.downField("payload").as[Json]
      eventId      <- quotedParsed.hcursor.downField("eventId").as[UUID]

      fixedPayload <- fix(innerPayload, columnToFix, fixedColumn)

    } yield IdAndEvent(eventId, fixedPayload)

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

  /** Print id and throw it away */
  def printEventId[F[_]: Monad: Logger, E](x: Either[E, IdAndEvent]): F[Either[E, Json]] =
    x match {
      case Right(IdAndEvent(id, payload)) =>
        Logger[F].info(s"Event id for recovery: $id").as(payload.asRight)
      case Left(error) =>
        Monad[F].pure(error.asLeft)
    }

  def writeListingToLogs[F[_]: Monad: Logger](p: Path): F[Path] =
    Logger[F].info(s"Processing file: ${p.fileName}, path: ${p.pathFromRoot}, isDir: ${p.isDir}").as(p)

  def keepTimePeriod(p: Path): Boolean =
    p.fileName.exists(_.startsWith("2020-11"))

  def keepInvalidColumnErrors(f: String): Boolean =
    f.contains("no such field.")
}
