package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.util.{ArrayList => JArrayList, Arrays => JArrays}

import cats.implicits._
import cats.effect.{ConcurrentEffect, Sync}

import io.circe.syntax._

import com.google.cloud.storage.{Acl, BlobInfo, StorageOptions}
import com.google.cloud.storage.Acl.Role
import com.google.cloud.storage.Acl.User

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import fs2.{Stream, text}
import fs2.io.toInputStream

import io.chrisdavenport.log4cats.Logger

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate

object Storage {
  val storage = StorageOptions.getDefaultInstance.getService

  val TimestampFormat = DateTimeFormat.forPattern("-YYYY-MM-dd-HHmmssSSS")
  val DefaultAcl = new JArrayList(JArrays.asList(Acl.of(User.ofAllUsers, Role.READER)))

  def getTime[F[_]: Sync]: F[DateTime] =
    Sync[F].delay(DateTime.now())

  def getFileName(base: String, n: Int, tstamp: DateTime): String =
    base ++ n.toString ++ DateTime.now(DateTimeZone.UTC).toString(TimestampFormat)

  def uploadFile[F[_]: ConcurrentEffect: Logger](bucketName: String, fileName: String)(rows: Stream[F, Desperate]): Stream[F, Unit] = {
    val blobInfo = BlobInfo.newBuilder(bucketName, fileName).setAcl(DefaultAcl).build()
    rows.map(_.asJson.noSpaces)
      .intersperse("\n")
      .through(text.utf8Encode)
      .through(toInputStream)
      .evalMap { is =>
        for {
          blob <- Sync[F].delay(storage.create(blobInfo, is))
          _ <- Logger[F].info(s"Written ${blob.getName} of ${blob.getSize} bytes")
        } yield ()
      }
  }
}
