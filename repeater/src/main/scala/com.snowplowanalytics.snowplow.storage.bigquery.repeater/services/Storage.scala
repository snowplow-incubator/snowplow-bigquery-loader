/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import com.google.cloud.storage.{BlobInfo, StorageOptions}

import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import fs2.{Chunk, Stream, text}

import com.snowplowanalytics.snowplow.badrows.BadRow

object Storage {
  // TODO: this is certainly non-RT
  private val storage = StorageOptions.getDefaultInstance.getService

  val TimestampFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HHmmssSSS")

  def getFileName(base: String, n: Int, tstamp: DateTime): String =
    base ++ DateTime.now(DateTimeZone.UTC).toString(TimestampFormat) ++ n.toString

  def uploadChunk[F[_]: Sync: Logger](bucketName: String, fileName: String, rows: Chunk[BadRow]): F[Unit] = {
    val blobInfo = BlobInfo.newBuilder(bucketName, fileName).build()
    val content  = Stream.chunk(rows).map(_.compact).intersperse("\n").through(text.utf8Encode).compile.to[Array]

    Logger[F].info(s"Preparing write to a $fileName with ${rows.size} items") *>
      Sync[F].delay(storage.create(blobInfo, content)) *>
      Logger[F].info(s"Written ${blobInfo.getName} of ${content.size} bytes")
  }
}
