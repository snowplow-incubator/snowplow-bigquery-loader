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
