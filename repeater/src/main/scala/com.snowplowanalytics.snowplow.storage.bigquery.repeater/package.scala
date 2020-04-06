package com.snowplowanalytics.snowplow.storage.bigquery

import com.permutive.pubsub.consumer.Model
import fs2.Stream
import fs2.concurrent.Queue

package object repeater {

  type EventRecord[F[_]] = Model.Record[F, EventContainer]

  type EventStream[F[_]] = Stream[F, EventRecord[F]]

  type EventQueue[F[_]] = Queue[F, EventRecord[F]]

}
