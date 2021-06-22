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

import io.circe.literal._

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.CliConfig.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Config.LoaderOutputs
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.LoadMode.StreamingInserts
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.{Config, Input, LoadMode, Output}

object SpecHelpers {
  val jsonResolver =
    json"""{"schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":500,"repositories":[{"name":"Iglu Central","priority":0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri":"http://iglucentral.com"}}}]}}"""

  val base64Resolver =
    "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="

  val base64Config =
    "ewogICJwcm9qZWN0SWQiOiAic25vd3Bsb3ctZGF0YSIKCiAgImxvYWRlciI6IHsKICAgICJpbnB1dCI6IHsKICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAic3Vic2NyaXB0aW9uIjogImVucmljaGVkLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6IHsKICAgICAgICAidHlwZSI6ICJCaWdRdWVyeSIKICAgICAgICAiZGF0YXNldElkIjogImF0b21pYyIKICAgICAgICAidGFibGVJZCI6ICJldmVudHMiCiAgICAgIH0KCiAgICAgICJiYWQiOiB7CiAgICAgICAgInR5cGUiOiAiUHViU3ViIgogICAgICAgICJ0b3BpYyI6ICJiYWQtdG9waWMiCiAgICAgIH0KCiAgICAgICJ0eXBlcyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogInR5cGVzLXRvcGljIgogICAgICB9CgogICAgICAiZmFpbGVkSW5zZXJ0cyI6IHsKICAgICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICAgInRvcGljIjogImZhaWxlZC1pbnNlcnRzLXRvcGljIgogICAgICB9CiAgICB9CgogICAgImxvYWRNb2RlIjogewogICAgICAidHlwZSI6ICJTdHJlYW1pbmdJbnNlcnRzIgogICAgICAicmV0cnkiOiBmYWxzZQogICAgfQogIH0KCiAgIm11dGF0b3IiOiB7CiAgICAiaW5wdXQiOiB7CiAgICAgICJ0eXBlIjogIlB1YlN1YiIKICAgICAgInN1YnNjcmlwdGlvbiI6ICJtdXRhdG9yLXN1YiIKICAgIH0KCiAgICAib3V0cHV0IjogewogICAgICAiZ29vZCI6ICR7bG9hZGVyLm91dHB1dC5nb29kfQogICAgfQogIH0KCiAgInJlcGVhdGVyIjogewogICAgImlucHV0IjogewogICAgICAidHlwZSI6ICJQdWJTdWIiCiAgICAgICJzdWJzY3JpcHRpb24iOiAiZmFpbGVkLWluc2VydHMtc3ViIgogICAgfQoKICAgICJvdXRwdXQiOiB7CiAgICAgICJnb29kIjogJHtsb2FkZXIub3V0cHV0Lmdvb2R9CgogICAgICAiZGVhZExldHRlcnMiOiB7CiAgICAgICAgInR5cGUiOiAiR2NzIgogICAgICAgICJidWNrZXQiOiAiZ3M6Ly9zb21lLWJ1Y2tldC8iCiAgICAgIH0KICAgIH0KICB9CgogICJ0YWdzIjogewogICAgImlkIjogImZmNTE3NmY4LWMwZTMtNGVmMC1hOTRmLTNiNGY4NmUwNDJjYSIKICAgICJuYW1lIjogIlNub3dwbG93IEJpZ1F1ZXJ5IgogIH0KfQ=="

  val subscription: String         = "enriched-sub"
  val input: Input.PubSub          = Input.PubSub(subscription)
  val datasetId: String            = "atomic"
  val tableId: String              = "events"
  val good: Output.BigQuery        = Output.BigQuery(datasetId, tableId)
  val badTopic: String             = "bad-topic"
  val bad: Output.PubSub           = Output.PubSub(badTopic)
  val typesTopic: String           = "types-topic"
  val types: Output.PubSub         = Output.PubSub(typesTopic)
  val failedInsertsTopic: String   = "failed-inserts-topic"
  val failedInserts: Output.PubSub = Output.PubSub(failedInsertsTopic)
  val output: LoaderOutputs        = LoaderOutputs(good, bad, types, failedInserts)
  val loadMode: LoadMode           = StreamingInserts(false)
  val loader: Config.Loader        = Config.Loader(input, output, loadMode)
  val projectId: String            = "snowplow-data"
  val loaderEnv: LoaderEnvironment = Environment(loader, jsonResolver, projectId)
}
