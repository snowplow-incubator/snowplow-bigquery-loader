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
package com.snowplowanalytics.snowplow.storage.bqmutator

import java.util.Base64

import com.google.api.services.bigquery.model.TableReference

import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

import cats.implicits._

import com.monovore.decline._

import Common._

case class Config(projectId: String, subscription: String, resolverJson: JValue, tableReference: TableReference)

object Config {

  val projectOpt = Opts.option[String]("project-id", "GCP Project Id (both PubSub and BigQuery)")
  val subscriptionOpt = Opts.option[String]("subscription", "PubSub Subscription providing alter table requests")
  val resolverOpt = Opts.option[String]("resolver", "Base64-encoded Iglu Resolver config").mapValidated { b64 =>
    catchNonFatalMessage(Base64.getDecoder.decode(b64))
      .map(new String(_))
      .andThen(s => catchNonFatalMessage(parse(s)))
  }

  val datasetIdOpt = Opts.option[String]("dataset-id", "BigQuery dataset id")
  val tableIdOpt = Opts.option[String]("table-id", "BigQuery table id")

  val command = Command("mutator", "Snowplow BigQuery Mutator") (
    (projectOpt, subscriptionOpt, resolverOpt, datasetIdOpt, tableIdOpt).mapN {
      (p, s, r, d, t) =>
        val tableRef = new TableReference().setProjectId(p).setDatasetId(d).setTableId(t)
        Config(p, s, r, tableRef)
    })


}
