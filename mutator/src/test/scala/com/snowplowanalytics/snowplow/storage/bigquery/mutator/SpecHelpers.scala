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

import io.circe.literal._
import io.circe.parser.parse

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

object SpecHelpers {
  val jsonResolver =
    json"""{"schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":500,"repositories":[{"name":"Iglu Central","priority":0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri":"http://iglucentral.com"}}}]}}"""

  val base64Resolver =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="

  val jsonConfig =
    json"""{"schema":"iglu:com.snowplowanalytics.snowplow.storage/bigquery_config/jsonschema/1-0-0","data":{"name":"Snowplow BigQuery","id":"ff5176f8-c0e3-4ef0-a94f-3b4f86e042ca","input":"enriched-topic","projectId":"snowplow-data","datasetId":"atomic","tableId":"events","typesTopic":"types-topic","typesSubscription":"types-sub", "badOutput": "gs://some-bucket/"}}"""

  val base64Config =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy5zdG9yYWdlL2JpZ3F1ZXJ5X2NvbmZpZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYW1lIjoiU25vd3Bsb3cgQmlnUXVlcnkiLCJpZCI6ImZmNTE3NmY4LWMwZTMtNGVmMC1hOTRmLTNiNGY4NmUwNDJjYSIsImlucHV0IjoiZW5yaWNoZWQtdG9waWMiLCJwcm9qZWN0SWQiOiJzbm93cGxvdy1kYXRhIiwiZGF0YXNldElkIjoiYXRvbWljIiwidGFibGVJZCI6ImV2ZW50cyIsInR5cGVzVG9waWMiOiJ0eXBlcy10b3BpYyIsInR5cGVzU3Vic2NyaXB0aW9uIjoidHlwZXMtc3ViIiwgImJhZE91dHB1dCI6ICJnczovL3NvbWUtYnVja2V0LyJ9fQ=="

  def parseSchema(string: String): Schema = {
    Schema
      .parse(parse(string).fold(throw _, identity))
      .getOrElse(throw new RuntimeException("SpecHelpers.parseSchema received invalid JSON Schema"))
  }
}
