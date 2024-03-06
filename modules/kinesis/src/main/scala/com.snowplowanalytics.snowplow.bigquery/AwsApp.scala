/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import com.snowplowanalytics.snowplow.sources.kinesis.{KinesisSource, KinesisSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kinesis.{KinesisSink, KinesisSinkConfig}

object AwsApp extends LoaderApp[KinesisSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def source: SourceProvider = KinesisSource.build(_)

  override def badSink: SinkProvider = KinesisSink.resource(_)
}
