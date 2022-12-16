/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery

import java.nio.charset.StandardCharsets

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data.ShreddedType
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.storage.bigquery.common.Codecs.toPayload
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Bigquery.FailedInsert

import com.permutive.pubsub.consumer.ConsumerRecord
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.producer.encoder.MessageEncoder

package object streamloader {

  type Payload[F[_]] = ConsumerRecord[F, String]

  implicit val messageDecoder: MessageDecoder[String] = (bytes: Array[Byte]) => {
    Right(new String(bytes, StandardCharsets.UTF_8))
  }

  implicit val badRowEncoder: MessageEncoder[BadRow] = { br =>
    Right(br.compact.getBytes(StandardCharsets.UTF_8))
  }

  implicit val shreddedTypesEncoder: MessageEncoder[Set[ShreddedType]] = { t =>
    Right(toPayload(t).noSpaces.getBytes(StandardCharsets.UTF_8))
  }

  implicit val messageEncoder: MessageEncoder[FailedInsert] = { tr =>
    Right(tr.tableRow.getBytes(StandardCharsets.UTF_8))
  }
}
