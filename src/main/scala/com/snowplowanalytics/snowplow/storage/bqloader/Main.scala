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

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import com.spotify.scio._

import schema.Schema

object Main {

  case class Config(input: String, tableRef: TableReference)

  def getConfg(args: Args): Config = {
    val input = args("input")
    val projectId = args("project-id")
    val datesetId = args("dataset-id")
    val tableId = args("table-id")

    val tableRef = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datesetId)
      .setTableId(tableId)

    Config(input, tableRef)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val config = getConfg(args)

//    EventsTable.create(config.tableRef)

     sc.pubsubTopic[String](config.input)
       .flatMap(Utils.parse)
       .saveAsBigQuery(config.tableRef,
         Schema.getAtomic,
         WriteDisposition.WRITE_APPEND,
         CreateDisposition.CREATE_IF_NEEDED,
         "atomic.events")

    val _ = sc.close().waitUntilDone()
  }
}
