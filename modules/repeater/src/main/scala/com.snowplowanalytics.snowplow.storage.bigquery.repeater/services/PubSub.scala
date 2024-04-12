/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import cats.effect._
import cats.effect.std.Queue
import cats.syntax.all._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.threeten.bp.{Duration => ThreetenDuration}
import com.google.pubsub.v1.PubsubMessage
import com.google.api.gax.core.ExecutorProvider
import com.google.api.gax.batching.FlowControlSettings
import com.google.common.util.concurrent.{ForwardingListeningExecutorService, MoreExecutors}

import com.permutive.pubsub.consumer.{ConsumerRecord, Model}
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.AllAppsConfig.GcpUserAgent
import com.snowplowanalytics.snowplow.storage.bigquery.common.createGcpUserAgentHeader
import com.snowplowanalytics.snowplow.storage.bigquery.repeater.{EventContainer, Repeater}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import java.util.concurrent.{Callable, ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

/** Module responsible for reading Pub/Sub */
object PubSub {

  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: Sync: Logger](
    projectId: String,
    subscription: String,
    uninsertable: Queue[F, BadRow],
    gcpUserAgent: GcpUserAgent,
    backoffPeriod: FiniteDuration
  ): Stream[F, ConsumerRecord[F, EventContainer]] =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      (msg, err, ack, _) => callback[F](msg, err, ack, uninsertable),
      PubsubGoogleConsumerConfig[F](
        onFailedTerminate = t => Logger[F].error(s"Terminating consumer due to $t"),
        customizeSubscriber = Some {
          _.setHeaderProvider(createGcpUserAgentHeader(gcpUserAgent))
            .setMaxAckExtensionPeriod(convertDuration(backoffPeriod.min(1.hour)))
            .setMinDurationPerAckExtension(convertDuration(backoffPeriod.min(600.seconds).minus(1.second)))
            .setExecutorProvider {
              new ExecutorProvider {
                def shouldAutoClose: Boolean              = true
                def getExecutor: ScheduledExecutorService = scheduledExecutorService
              }
            }
            .setFlowControlSettings {
              // Switch off any flow control, because we handle it ourselves via fs2's backpressure
              FlowControlSettings.getDefaultInstance
            }
        }
      )
    )

  private def convertDuration(d: FiniteDuration): ThreetenDuration =
    ThreetenDuration.ofMillis(d.toMillis)

  def scheduledExecutorService: ScheduledExecutorService =
    new ForwardingListeningExecutorService with ScheduledExecutorService {
      val delegate       = MoreExecutors.newDirectExecutorService
      lazy val scheduler = new ScheduledThreadPoolExecutor(1) // I think this scheduler is never used, but I implement it here for safety
      override def schedule[V](
        callable: Callable[V],
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[V] =
        scheduler.schedule(callable, delay, unit)
      override def schedule(
        runnable: Runnable,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        scheduler.schedule(runnable, delay, unit)
      override def scheduleAtFixedRate(
        runnable: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        scheduler.scheduleAtFixedRate(runnable, initialDelay, period, unit)
      override def scheduleWithFixedDelay(
        runnable: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        scheduler.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)
      override def shutdown(): Unit = {
        delegate.shutdown()
        scheduler.shutdown()
      }
    }

  private def callback[F[_]: Sync](msg: PubsubMessage, err: Throwable, ack: F[Unit], uninsertable: Queue[F, BadRow]) = {
    val info    = FailureDetails.LoaderRecoveryError.ParsingError(err.toString, Nil)
    val failure = Failure.LoaderRecoveryFailure(info)
    val badRow  = BadRow.LoaderRecoveryError(Repeater.processor, failure, Payload.RawPayload(msg.toString))
    uninsertable.offer(badRow) >> ack
  }
}
