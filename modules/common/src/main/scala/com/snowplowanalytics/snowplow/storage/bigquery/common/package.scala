package com.snowplowanalytics.snowplow.storage.bigquery

import com.google.api.gax.rpc.FixedHeaderProvider
import com.snowplowanalytics.iglu.client.resolver.StorageTime
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.lrumap.LruMap
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.AllAppsConfig.GcpUserAgent

package object common {
  type FieldKey         = (SchemaKey, StorageTime)
  type FieldCache[F[_]] = LruMap[F, FieldKey, Field]

  def createGcpUserAgentHeader(gcpUserAgent: GcpUserAgent): FixedHeaderProvider =
    FixedHeaderProvider.create("user-agent", s"${gcpUserAgent.productName}/bigquery-loader (GPN:Snowplow;)")

}
