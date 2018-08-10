package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.spotify.scio.bigquery.TableRow

import org.apache.beam.sdk.transforms.SerializableFunction

object SerializeLoaderRow extends SerializableFunction[LoaderRow, TableRow] {
  def apply(input: LoaderRow): TableRow =
    input.data
}

