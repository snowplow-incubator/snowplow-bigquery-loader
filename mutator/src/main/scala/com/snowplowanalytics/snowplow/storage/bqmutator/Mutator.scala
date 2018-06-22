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

import scala.collection.convert.decorateAsScala._
import scala.collection.convert.decorateAsJava._

import cats.syntax.either._

import org.json4s.jackson.JsonMethods.fromJsonNode

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, StandardTableDefinition, Field, LegacySQLTypeName, Schema => BqSchema }
import com.google.api.services.bigquery.model.TableReference

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.{IgluUri, InventoryItem, fixSchema}

class Mutator private(resolver: Resolver, tableRef: TableReference) {

  private val client: BigQuery = BigQueryOptions.getDefaultInstance.getService

  def updateTable(inventoryItem: InventoryItem): Unit = {
    val columnName = fixSchema(inventoryItem.shredProperty, inventoryItem.igluUri)
    val table = client.getTable(tableRef.getDatasetId, tableRef.getTableId)
    val definition = table.getDefinition[StandardTableDefinition]
    val tableSchema = definition.getSchema.getFields.iterator().asScala.toVector

    tableSchema.find(_.getName == columnName) match {
      case Some(_) => ()
      case None =>
        // val schema = getSchema(inventoryItem.igluUri).map(Schema.parse(_))
        val newField = Field.newBuilder(columnName, LegacySQLTypeName.STRING).build()
        val newSchema = BqSchema.of((tableSchema :+ newField).asJava)
        val updatedDefinition = definition.toBuilder.setSchema(newSchema).build()

        println(s"Adding $columnName")
        table.toBuilder.setDefinition(updatedDefinition).build().update()
    }
  }

  def getSchema(igluUri: IgluUri) = {
    val result = resolver.lookupSchema(igluUri)
    Common
      .fromValidation(result)
      .map(fromJsonNode)
  }
}

object Mutator {
  def initialize(config: Config): Either[String, Mutator] = {
    Common.fromValidation(Resolver.parse(config.resolverJson)) match {
      case Right(resolver) => new Mutator(resolver, config.tableReference).asRight
      case Left(errors) => errors.toList.mkString(", ").asLeft
    }
  }
}
