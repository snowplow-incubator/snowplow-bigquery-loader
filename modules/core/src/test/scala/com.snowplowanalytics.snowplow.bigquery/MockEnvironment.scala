/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream
import com.google.protobuf.Descriptors

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.bigquery.processing.{TableManager, WriterProvider}
import com.snowplowanalytics.snowplow.runtime.AppInfo

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case object CreatedTable extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Long) extends Action
    case class AlterTableAddedColumns(columns: List[String]) extends Action
    case object OpenedWriter extends Action
    case object ClosedWriter extends Action
    case class WroteRowsToBigQuery(rowCount: Int) extends Action
    case class AddedGoodCountMetric(count: Long) extends Action
    case class AddedBadCountMetric(count: Long) extends Action
    case class SetLatencyMetric(millis: Long) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @param writerResponses
   *   Responses we want the `WriterProvider` to return when someone calls `write`
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents], writerResponses: List[WriterProvider.WriteResult] = Nil): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      writerProvider <- testWriterProvider(state, writerResponses)
    } yield {
      val env = Environment(
        appInfo        = appInfo,
        source         = testSourceAndAck(inputs, state),
        badSink        = testSink(state),
        resolver       = Resolver[IO](Nil, None),
        httpClient     = testHttpClient,
        tableManager   = testTableManager(state),
        writerProvider = writerProvider,
        metrics        = testMetrics(state),
        batching = Config.Batching(
          maxBytes              = 16000000,
          maxDelay              = 10.seconds,
          writeBatchConcurrency = 1
        ),
        badRowMaxSize = 1000000
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name        = "snowflake-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/snowflake-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testTableManager(state: Ref[IO, Vector[Action]]): TableManager[IO] = new TableManager[IO] {
    def addColumns(columns: List[Field]): IO[Unit] =
      state.update(_ :+ AlterTableAddedColumns(columns.map(_.name)))

    def createTable: IO[Unit] =
      state.update(_ :+ CreatedTable)
  }

  private def testSourceAndAck(inputs: List[TokenedEvents], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .emits(inputs)
          .through(processor)
          .chunks
          .evalMap { chunk =>
            state.update(_ :+ Checkpointed(chunk.toList))
          }
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  /**
   * Mocked implementation of a `WriterProvider`
   *
   * @param actionRef
   *   Global Ref used to accumulate actions that happened
   * @param responses
   *   Responses that this mocked WriterProvider should return each time someone calls `write`. If
   *   no responses given, then it will return with a successful response.
   */
  private def testWriterProvider(
    actionRef: Ref[IO, Vector[Action]],
    responses: List[WriterProvider.WriteResult]
  ): IO[Resource[IO, WriterProvider[IO]]] =
    for {
      responseRef <- Ref[IO].of(responses)
    } yield {
      val make = actionRef.update(_ :+ OpenedWriter).as {
        new WriterProvider[IO] {
          def descriptor: IO[Descriptors.Descriptor] =
            IO(AtomicDescriptor.get)

          def write(rows: List[Map[String, AnyRef]]): IO[WriterProvider.WriteResult] =
            for {
              response <- responseRef.modify {
                            case head :: tail => (tail, head)
                            case Nil          => (Nil, WriterProvider.WriteResult.Success)
                          }
              _ <- response match {
                     case WriterProvider.WriteResult.Success =>
                       actionRef.update(_ :+ WroteRowsToBigQuery(rows.size))
                     case _ =>
                       IO.unit
                   }
            } yield response
        }
      }

      Resource.make(make)(_ => actionRef.update(_ :+ ClosedWriter))
    }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Long): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Long): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def setLatencyMillis(latencyMillis: Long): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latencyMillis))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
