package com.snowplowanalytics.snowplow.storage.bigquery.streamloader

import cats.Applicative

import scala.concurrent.duration._
import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import io.circe.Json
import com.google.cloud.bigquery.BigQuery
import com.permutive.pubsub.producer.Model
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.Environment
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.{Bigquery, PubSub}
import com.snowplowanalytics.snowplow.storage.bigquery.streamloader.Sinks.PubSub.{Producer, PubSubOutput}

class Resources[F[_]](val pubsub: PubSub.Producer[F],
                      val bigQuery: BigQuery,
                      val igluClient: Client[F, Json],
                      val env: Environment)

object Resources {
  def acquire[F[_]: Async: InitSchemaCache: InitListCache](env: Environment): Resource[F, Resources[F]] = {
    val clientF: F[Client[F, Json]] = Client.parseDefault[F](env.resolverJson).value.flatMap {
      case Right(client) =>
        Applicative[F].pure(client)
      case Left(error) =>
        Sync[F].raiseError[Client[F, Json]](new RuntimeException(s"Cannot decode Iglu Client: ${error.show}"))
    }

    for {
      pubsub <- mkProducer[F](env.config.projectId, env.config.failedInserts)
      bigquery <- Resource.liftF(Bigquery.getClient)
      client <- Resource.liftF(clientF)
    } yield new Resources[F](pubsub, bigquery, client, env)
  }


  def mkProducer[F[_]: Async](projectId: String, topic: String): Resource[F, Producer[F]] =
    GooglePubsubProducer
      .of[F, PubSubOutput](
        Model.ProjectId(projectId),
        Model.Topic(topic),
        config = PubsubProducerConfig[F](
          // TODO: Get rid of magic numbers
          batchSize         = 100,
          delayThreshold    = 100.millis,
          onFailedTerminate = e => Sync[F].delay(println(s"Got error $e")).void
        )
      )
}
