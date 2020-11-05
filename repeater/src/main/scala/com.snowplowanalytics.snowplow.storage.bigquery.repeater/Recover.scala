package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import fs2.Stream
import cats.effect.Concurrent
import cats.effect.Timer
import blobstore.Path

object Recover {

  def recoverFailedInserts[F[_]: Timer: Concurrent](resources: Resources[F]): Stream[F, Unit] =
    for {
      readFromGcs <- resources.store.get(Path("gs://foo/bar"), 1024)
      _           <- Stream.eval(resources.pubSubProducer.produce(recover(readFromGcs)))
    } yield ()

  private def recover: Byte => EventContainer = ???

}
