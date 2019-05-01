package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import java.util.{ArrayList => JArrayList, Arrays => JArrays}

import cats.implicits._
import cats.effect.{ConcurrentEffect, Sync}

import com.google.cloud.storage.Acl.{Role, User}
import com.google.cloud.storage.{Acl, BlobInfo, StorageOptions}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate

import fs2.io.toInputStream
import fs2.{Stream, text, Chunk}

import io.chrisdavenport.log4cats.Logger

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import io.circe.syntax._

object Storage {

  // TODO: this is certainly non-RT
  private val storage = StorageOptions.getDefaultInstance.getService

  val TimestampFormat = DateTimeFormat.forPattern("-YYYY-MM-dd-HHmmssSSS")
  val DefaultAcl = new JArrayList(JArrays.asList(Acl.of(User.ofAllUsers, Role.READER)))

  def getFileName(base: String, n: Int, tstamp: DateTime): String =
    base ++ n.toString ++ DateTime.now(DateTimeZone.UTC).toString(TimestampFormat)

  def uploadChunk[F[_]: ConcurrentEffect: Logger](bucketName: String, fileName: String)(rows: Chunk[Desperate]): F[Unit] = {
    val blobInfo = BlobInfo.newBuilder(bucketName, fileName).setAcl(DefaultAcl).build()
    Stream.chunk(rows)
      .map(_.asJson.noSpaces)
      .intersperse("\n")
      .through(text.utf8Encode)
      .through(toInputStream)
      .evalTap { is =>
        for {
          blob <- Sync[F].delay(storage.create(blobInfo, is))
          _ <- Logger[F].info(s"Written ${blob.getName} of ${blob.getSize} bytes")
        } yield ()

      }
      .compile
      .drain
  }
}
