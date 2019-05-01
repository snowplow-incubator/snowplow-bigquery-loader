package com.snowplowanalytics.snowplow.storage.bigquery.repeater.services

import cats.syntax.all._
import cats.effect._

import org.http4s.client.Client

import io.chrisdavenport.log4cats.Logger

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.http.{PubsubHttpConsumer, PubsubHttpConsumerConfig}

import com.snowplowanalytics.snowplow.storage.bigquery.repeater.EventContainer

/** Module responsible for reading PubSub */
object PubSub {
  /** Read events from `failedInserts` topic */
  def getEvents[F[_]: Concurrent: Timer: Logger](projectId: String,
                                                 serviceAccount: String,
                                                 subscription: String,
                                                 client: Client[F]) =
    PubsubHttpConsumer.subscribe[F, EventContainer](
      Model.ProjectId(projectId),
      Model.Subscription(subscription),
      serviceAccount,
      PubsubHttpConsumerConfig[F](
        host = "pubsub.googleapis.com",
        readReturnImmediately = true,
        port = 443,
        readConcurrency = 2
      ),
      client,
      (msg, err, ack, _) => Logger[F].error(s"Msg $msg got error $err") >> ack,
    )
}
