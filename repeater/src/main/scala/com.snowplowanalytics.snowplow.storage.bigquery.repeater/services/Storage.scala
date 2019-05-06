package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import java.util.{ArrayList => JArrayList, Arrays => JArrays}

import cats.implicits._
import cats.effect.Sync

import com.google.cloud.storage.Acl.{Role, User}
import com.google.cloud.storage.{Acl, BlobInfo, StorageOptions}
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer.Desperate

import fs2.Chunk

import io.chrisdavenport.log4cats.Logger

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import io.circe.syntax._

object Storage {

  // TODO: this is certainly non-RT
  private val storage = StorageOptions.getDefaultInstance.getService

  val TimestampFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HHmmssSSS")
  val DefaultAcl = new JArrayList(JArrays.asList(Acl.of(User.ofAllUsers, Role.READER)))

  def getFileName(base: String, n: Int, tstamp: DateTime): String =
    base ++ DateTime.now(DateTimeZone.UTC).toString(TimestampFormat) ++ n.toString

  def uploadChunk[F[_]: Sync: Logger](bucketName: String, fileName: String, rows: Chunk[Desperate]): F[Unit] = {
    val blobInfo = BlobInfo.newBuilder(bucketName, fileName).setAcl(DefaultAcl).build()
    val content = rows
      .toChain
      .map(_.asJson.noSpaces)
      .mkString_("", "\n", "")
      .getBytes()

    Logger[F].info(s"Preparing write to a $fileName with ${rows.size} items") *>
      Sync[F].delay(storage.create(blobInfo, content)) *>
      Logger[F].info(s"Written ${blobInfo.getName} of ${content.size} bytes")
  }
}
