package com.snowplowanalytics.snowplow.storage.bqmutator

import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._

import cats.effect.IO

import com.google.cloud.bigquery.{BigQuery, StandardTableDefinition, Table, Field, Schema => BqSchema }

/** Stateless object, responsible for making API calls to a table or mock */
trait TableReference {
  def getFields: IO[Vector[Field]]
  def addField(field: Field): IO[Unit]
}

object TableReference {
  class BigQueryTable(client: BigQuery, datasetId: String, tableId: String) extends TableReference {

    def getTable: IO[Table] =
      IO(client.getTable(datasetId, tableId))

    def getFields: IO[Vector[Field]] =
      getTable.map(BigQueryTable.getFields)

    def addField(field: Field): IO[Unit] = {
      for {
        table <- getTable
        originalFields = BigQueryTable.getFields(table)
        newSchema = BqSchema.of((originalFields :+ field).asJava)
        updatedDefinition = table.getDefinition[StandardTableDefinition].toBuilder.setSchema(newSchema).build()
        updatedTable = table.toBuilder.setDefinition(updatedDefinition).build()
        _ <- IO(updatedTable.update())
      } yield ()
    }
  }

  object BigQueryTable {
    private def getFields(table: Table): Vector[Field] =
      table.getDefinition[StandardTableDefinition]
        .getSchema
        .getFields
        .iterator()
        .asScala
        .toVector
  }
}

