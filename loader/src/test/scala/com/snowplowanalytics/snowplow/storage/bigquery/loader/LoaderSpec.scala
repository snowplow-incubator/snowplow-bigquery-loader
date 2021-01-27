/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.spotify.scio.testing.PipelineSpec

class LoaderSpec extends PipelineSpec {
  "Loader" should "split inputs" in {
    // TODO
  }
}

object LoaderSpec {
  val resolverJson =
    """{"schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":500,"repositories":[{"name":"Iglu Central","priority":0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri":"http://iglucentral.com"}}}]}}"""
  val resolver = java.util.Base64.getEncoder.encode(resolverJson.getBytes())

  val configJson =
    """{"schema":"iglu:com.snowplowanalytics.snowplow.storage/bigquery_config/jsonschema/1-0-0","data":{"name":"Alpha BigQuery test","id":"91a251ad-d319-4023-aaae-97698238d808","projectId":"test-sandbox","datasetId":"atomic","tableId":"events","load":{"mode":"STREAMING_INSERTS","retry":false},"typesTopic":"bq-test-types","typesSubscription":"bq-test-types-sub","input":"enriched-good-sub","badRows":"bq-test-bad-rows","failedInserts":"bq-test-bad-inserts","purpose":"ENRICHED_EVENTS"}}"""
  val config = java.util.Base64.getEncoder.encode(configJson.getBytes())
}
