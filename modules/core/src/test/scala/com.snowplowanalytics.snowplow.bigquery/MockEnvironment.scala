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
import com.google.cloud.bigquery.FieldList

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.runtime.AppInfo
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.bigquery.processing.{BigQueryRetrying, TableManager, Writer}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  private val everythingHealthy: Map[AppHealth.Service, Boolean] = Map(
    AppHealth.Service.BigQueryClient -> true,
    AppHealth.Service.BadSink -> true
  )

  sealed trait Action
  object Action {
    case object CreatedTable extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Long) extends Action
    case class AlterTableAddedColumns(columns: Vector[String]) extends Action
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
   * @param mocks
   *   Responses we want the `Writer` to return when someone calls uses the mocked services
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents], mocks: Mocks): Resource[IO, MockEnvironment] =
    for {
      state <- Resource.eval(Ref[IO].of(Vector.empty[Action]))
      writerResource <- Resource.eval(testWriter(state, mocks.writerResponses, mocks.descriptors))
      writerColdswap <- Coldswap.make(writerResource)
      source = testSourceAndAck(inputs, state)
      appHealth <- Resource.eval(AppHealth.init(10.seconds, source, everythingHealthy))
    } yield {
      val env = Environment(
        appInfo              = appInfo,
        source               = source,
        badSink              = testBadSink(mocks.badSinkResponse, state),
        resolver             = Resolver[IO](Nil, None),
        httpClient           = testHttpClient,
        tableManager         = testTableManager(mocks.addColumnsResponse, state),
        writer               = writerColdswap,
        metrics              = testMetrics(state),
        appHealth            = appHealth,
        alterTableWaitPolicy = BigQueryRetrying.policyForAlterTableWait[IO](retriesConfig),
        batching = Config.Batching(
          maxBytes              = 16000000,
          maxDelay              = 10.seconds,
          writeBatchConcurrency = 1
        ),
        badRowMaxSize = 1000000,
        schemasToSkip = List.empty,
        legacyColumns = false
      )
      MockEnvironment(state, env)
    }

  final case class Mocks(
    writerResponses: List[Response[Writer.WriteResult]],
    badSinkResponse: Response[Unit],
    addColumnsResponse: Response[FieldList],
    descriptors: List[Descriptors.Descriptor]
  )

  object Mocks {
    val default: Mocks = Mocks(
      writerResponses    = List.empty,
      badSinkResponse    = Response.Success(()),
      addColumnsResponse = Response.Success(FieldList.of()),
      descriptors        = List.empty
    )
  }

  sealed trait Response[+A]
  object Response {
    final case class Success[A](value: A) extends Response[A]
    final case class ExceptionThrown(value: Throwable) extends Response[Nothing]
  }

  val appInfo = new AppInfo {
    def name        = "bigquery-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/bigquery-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testTableManager(mockedResponse: Response[FieldList], state: Ref[IO, Vector[Action]]): TableManager.WithHandledErrors[IO] =
    new TableManager.WithHandledErrors[IO] {
      def addColumns(columns: Vector[Field]): IO[FieldList] =
        mockedResponse match {
          case Response.Success(fieldList) =>
            state.update(_ :+ AlterTableAddedColumns(columns.map(_.name))).as(fieldList)
          case Response.ExceptionThrown(value) =>
            IO.raiseError(value)
        }

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

  private def testBadSink(mockedResponse: Response[Unit], state: Ref[IO, Vector[Action]]): Sink[IO] =
    Sink[IO] { batch =>
      mockedResponse match {
        case Response.Success(_) =>
          state.update(_ :+ SentToBad(batch.size))
        case Response.ExceptionThrown(value) =>
          IO.raiseError(value)
      }
    }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  /**
   * Mocked implementation of a `Writer`
   *
   * @param actionRef
   *   Global Ref used to accumulate actions that happened
   * @param responses
   *   Responses that this mocked Writer should return each time someone calls `write`. If no
   *   responses given, then it will return with a successful response.
   */
  private def testWriter(
    actionRef: Ref[IO, Vector[Action]],
    responses: List[Response[Writer.WriteResult]],
    descriptors: List[Descriptors.Descriptor]
  ): IO[Resource[IO, Writer[IO]]] =
    for {
      responseRef <- Ref[IO].of(responses)
      descriptorRef <- Ref[IO].of(descriptors)
    } yield {
      val make = actionRef.update(_ :+ OpenedWriter).as {
        new Writer[IO] {
          def descriptor: IO[Descriptors.Descriptor] =
            descriptorRef.modify {
              case head :: tail => (tail, head)
              case Nil          => (Nil, AtomicDescriptor.withWebPage)
            }

          def write(rows: List[Map[String, AnyRef]]): IO[Writer.WriteResult] =
            for {
              response <- responseRef.modify {
                            case head :: tail => (tail, head)
                            case Nil          => (Nil, Response.Success(Writer.WriteResult.Success))
                          }
              writeResult <- response match {
                               case success: Response.Success[Writer.WriteResult] =>
                                 updateActions(actionRef, rows, success) *> IO(success.value)
                               case Response.ExceptionThrown(ex) =>
                                 IO.raiseError(ex)
                             }
            } yield writeResult

          private def updateActions(
            state: Ref[IO, Vector[Action]],
            rows: Iterable[Map[String, AnyRef]],
            success: Response.Success[Writer.WriteResult]
          ): IO[Unit] =
            success.value match {
              case Writer.WriteResult.Success =>
                state.update(_ :+ WroteRowsToBigQuery(rows.size))
              case _ =>
                IO.unit
            }
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

  def retriesConfig = Config.Retries(
    Config.SetupErrorRetries(30.seconds),
    Config.TransientErrorRetries(1.second, 5),
    Config.AlterTableWaitRetries(1.second),
    Config.TooManyColumnsRetries(300.seconds)
  )
}
