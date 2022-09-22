package com.snowplowanalytics.snowplow.storage.bigquery

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.lrumap.LruMap

package object common {
  //todo - if after the high level perf test we decide to make this change, then this should be come (SchemaKey, Int) to
  //apply a TTL with the same pattern we use elsewhere
  type FieldKey = SchemaKey
  type LookupProperties[F[_]] = LruMap[F, FieldKey, Field]
}
