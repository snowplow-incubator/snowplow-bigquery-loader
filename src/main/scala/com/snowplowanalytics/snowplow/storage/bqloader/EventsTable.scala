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

import org.apache.beam.sdk.io.gcp.bigquery._
import org.apache.beam.sdk.options.PipelineOptionsFactory

import com.google.api.services.bigquery.model.TableReference

object EventsTable {
  def create(tableRef: TableReference): Unit = {
    val options = PipelineOptionsFactory.create().as(classOf[BigQueryOptions])
    val service = new BigQueryServicesWrapper(options)
    service.createTable(tableRef, schema.Schema.getSchema)
  }
}
