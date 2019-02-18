/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import io.circe.Json

import org.specs2.Specification

import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaVer }


class TypeReceiverSpec extends Specification { def is = s2"""
  TypeReceiver decodes legacy inventory items $e1
  """

  def e1 = {
    val input = Json.fromValues(List(
      Json.fromFields(List(
        "schema" -> Json.fromString("iglu:com.snowplowanalytics/event/jsonschema/1-0-0"),
        "type" -> Json.fromString("UNSTRUCT_EVENT")
      )),
      Json.fromFields(List(
        "schema" -> Json.fromString("iglu:com.snowplowanalytics/context/jsonschema/1-0-0"),
        "type" -> Json.fromString("CONTEXTS")
      ))
    ))

    val result = TypeReceiver.decodeItems(input)
    val expected = List(
      ShreddedType(UnstructEvent, SchemaKey("com.snowplowanalytics","event","jsonschema",SchemaVer.Full(1,0,0))),
      ShreddedType(Contexts(CustomContexts), SchemaKey("com.snowplowanalytics","context","jsonschema",SchemaVer.Full(1,0,0)))
    )

    result must beRight(expected)
  }
}
