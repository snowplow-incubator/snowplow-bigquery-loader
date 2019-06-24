/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

object BadRowSchemas {
  val LoaderParsingError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_parsing_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val LoaderIgluError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_iglu_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val CastError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bq_cast_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val LoaderRuntimeError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bq_loader_runtime_error", "jsonschema", SchemaVer.Full(1, 0, 0))

  val RepeaterParsingError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bq_repeater_parsing_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val RepeaterBQError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bq_repeater_bigquery_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val RepeaterPubSubError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bq_repeater_pubsub_error", "jsonschema", SchemaVer.Full(1, 0, 0))
}
