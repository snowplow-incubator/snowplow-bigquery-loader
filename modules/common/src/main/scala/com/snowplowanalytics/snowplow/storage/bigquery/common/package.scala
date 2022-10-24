package com.snowplowanalytics.snowplow.storage.bigquery

import com.snowplowanalytics.iglu.client.resolver.TTL
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.lrumap.LruMap

package object common {
  type ColumnName = String
  type FieldKey = (SchemaKey, TTL)
  type LookupProperties[F[_]] = LruMap[F, FieldKey, FieldValue]
}
