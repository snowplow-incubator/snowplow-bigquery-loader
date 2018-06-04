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
package com.snowplowanalytics.snowplow.storage.bqloader

import com.google.api.services.bigquery.model.TableReference

import com.spotify.scio.Args

case class Config(input: String, types: String, tableRef: TableReference)

object Config {
  def parse(args: Args): Config = {
    val input = args("subscription")

    val projectId = args("project-id")
    val datesetId = args("dataset-id")
    val tableId = args("table-id")

    val typesTopic = args("types-topic")

    val tableRef = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datesetId)
      .setTableId(tableId)

    Config(input, typesTopic, tableRef)
  }
}
