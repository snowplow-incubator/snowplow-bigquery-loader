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

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.metrics.Metrics

import cats.effect.Sync
import cats.implicits._
import com.google.cloud.storage.{BlobInfo, StorageOptions, StorageRetryStrategy}
import fs2.{Chunk, Stream, text}
import org.typelevel.log4cats.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

object Storage {
  // TODO: this is certainly non-RT
  private val storage = {
    StorageOptions.newBuilder
      .setStorageRetryStrategy(StorageRetryStrategy.getUniformStorageRetryStrategy)
      .build
      .getService
  }

  private val TimestampFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HHmmssSSS")

  def getFileName(base: String, n: Int): String =
    base ++ DateTime.now(DateTimeZone.UTC).toString(TimestampFormat) ++ n.toString

  def uploadChunk[F[_]: Sync: Logger](
    bucketName: String,
    fileName: String,
    rows: Chunk[BadRow],
    metrics: Metrics[F]
  ): F[Unit] = {
    val blobInfo = BlobInfo.newBuilder(bucketName, fileName).build()
    val content  = Stream.chunk(rows).map(_.compact).intersperse("\n").through(text.utf8.encode).compile.to(Array)

    // format: off
    Logger[F].info(s"Preparing write to $fileName with ${rows.size} items") *>
      Sync[F].delay(storage.create(blobInfo, content)) *> metrics.uninsertableCount(rows.size) *>
      Logger[F].info(s"Written ${blobInfo.getName} of ${content.size} bytes")
    // format: on
  }
}
