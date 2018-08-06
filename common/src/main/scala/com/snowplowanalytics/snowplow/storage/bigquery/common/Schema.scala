package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

object Schema {
  /** Convert SchemaKey into BigQuery-compatible column name */
  def getColumnName(inventoryItem: InventoryItem): String =
    SchemaKey.fromUri(inventoryItem.igluUri) match {
      case Some(SchemaKey(vendor, name, _, SchemaVer.Full(m, r, a))) =>
        val vendor_ = vendor.replaceAll("""[\.\-]""", "_").toLowerCase
        val name_ = normalizeCase(name)
        val version = s"${m}_${r}_$a"
        s"${inventoryItem.shredProperty.prefix}${vendor_}_${name_}_$version"
      case _ =>   // TODO: https://github.com/snowplow/iglu/issues/364
        throw new RuntimeException(s"Iglu URI ${inventoryItem.igluUri} is not yet supported")
    }

  def normalizeCase(string: String): String =
    string
      .replaceAll("""[\.\-]""", "_")
      .replaceAll("([^A-Z_])([A-Z])", "$1_$2")
      .toLowerCase
}
