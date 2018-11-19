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
package com.snowplowanalytics.snowplow.storage.bigquery
package forwarder

import com.spotify.scio._

import org.apache.beam.sdk.options.PipelineOptionsFactory

object Main {
  def main(args: Array[String]): Unit = {
    PipelineOptionsFactory.register(classOf[CommandLine.Options])
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation
      .as(classOf[CommandLine.Options])
    options.setStreaming(true)
    val sc = ScioContext(options)
    val env = CommandLine.getEnvironment(options)
    Forwarder.run(env, sc)
    val _ = sc.close()
  }
}
