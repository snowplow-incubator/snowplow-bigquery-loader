package com.snowplowanalytics.snowplow.storage.bigquery

import com.snowplowanalytics.iglu.client.resolver.StorageTime
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.lrumap.LruMap

package object common {
  type FieldKey = (SchemaKey, StorageTime)
  type FieldCache[F[_]] = LruMap[F, FieldKey, Field]
}
