/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import com.snowplowanalytics.snowplow.sources.pubsub.{PubsubSource, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.sinks.pubsub.{PubsubSink, PubsubSinkConfig}

object GcpApp extends LoaderApp[PubsubSourceConfig, PubsubSinkConfig](BuildInfo) {

  override def source: SourceProvider = PubsubSource.build(_)

  override def badSink: SinkProvider = PubsubSink.resource(_)
}
