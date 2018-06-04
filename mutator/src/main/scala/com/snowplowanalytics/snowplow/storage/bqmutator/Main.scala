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
package com.snowplowanalytics.snowplow.storage.bqmutator

import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName

import cats.implicits._

import com.monovore.decline._

object Main {
  case class Config(projectId: String, subscription: String)

  def main(args: Array[String]): Unit = {
    val projectOpt = Opts.option[String]("project-id", "GCP Project Id")
    val subscriptionOpt = Opts.option[String]("subscription", "PubSub Subscription providing alter table requests")

    val command = Command("mutator", "Snowplow BigQuery Mutator") ((projectOpt, subscriptionOpt).mapN {
      (p, s) => Config(p, s)
    })

    command.parse(args) match {
      case Right(config) =>
        val subscription = ProjectSubscriptionName.of(config.projectId, config.subscription)
        val subscriber = Subscriber
          .newBuilder(subscription, Listener)
          .build()

        subscriber.startAsync().awaitTerminated()
        subscriber.awaitRunning()
      case Left(error) =>
        System.err.println(error)
        System.exit(1)
    }
  }
}
