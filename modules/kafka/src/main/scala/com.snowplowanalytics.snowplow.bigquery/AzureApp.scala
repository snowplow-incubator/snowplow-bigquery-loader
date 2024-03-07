/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import com.snowplowanalytics.snowplow.azure.AzureAuthenticationCallbackHandler
import com.snowplowanalytics.snowplow.sources.kafka.{KafkaSource, KafkaSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kafka.{KafkaSink, KafkaSinkConfig}

import scala.reflect.classTag

class SourceAuthHandler extends AzureAuthenticationCallbackHandler

class SinkAuthHandler extends AzureAuthenticationCallbackHandler

object AzureApp extends LoaderApp[KafkaSourceConfig, KafkaSinkConfig](BuildInfo) {

  override def source: SourceProvider = KafkaSource.build(_, classTag[SourceAuthHandler])

  override def badSink: SinkProvider = KafkaSink.resource(_, classTag[SinkAuthHandler])
}
