/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration.Duration
import com.spotify.scio._

@deprecated("This component is no longer supported", "Forwarder 0.5.0")
object Main {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    ForwarderCli.parse(args) match {
      case Right(env) =>
        Forwarder.run(env, sc)
        val _ = sc.run().waitUntilDone(Duration.Inf)
      case Left(error) =>
        System.err.println(error.getMessage)
        System.exit(1)
    }
  }
}
