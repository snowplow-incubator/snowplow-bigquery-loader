package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import cats.syntax.all._
import cats.effect._

import io.chrisdavenport.log4cats.Logger

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig }

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer

/** Module responsible for reading PubSub */
object PubSub {
  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: ContextShift: Concurrent: Timer: Logger](projectId: String, subscription: String) =
    PubsubGoogleConsumer.subscribe[F, EventContainer](
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      (msg, err, ack, _) => Logger[F].error(s"Msg $msg got error $err") >> ack,
      PubsubGoogleConsumerConfig[F](onFailedTerminate = t => Logger[F].error(s"Terminating consumer due $t"))
    )
}
