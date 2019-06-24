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
  val LoaderInternalError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bigquery_loader_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val RepeaterParsingError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bigquery_repeater_parsing_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val RepeaterInternalError = SchemaKey("com.snowplowanalytics.snowplow.badrows", "bigquery_repeater_internal_error", "jsonschema", SchemaVer.Full(1, 0, 0))
}
