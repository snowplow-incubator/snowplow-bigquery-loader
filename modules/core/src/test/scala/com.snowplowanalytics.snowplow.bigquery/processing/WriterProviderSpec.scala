/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.implicits._
import cats.effect.{IO, Ref}
import cats.effect.std.Supervisor
import com.google.api.gax.rpc.PermissionDeniedException
import com.google.api.gax.grpc.GrpcStatusCode
import com.google.protobuf.Descriptors
import io.grpc.Status
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import com.snowplowanalytics.snowplow.bigquery.{Alert, AppHealth, AtomicDescriptor, Config, Monitoring}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.bigquery.AppHealth.Service
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck}

class WriterProviderSpec extends Specification with CatsEffect {
  import WriterProviderSpec._

  def is = s2"""
  The writer provider should
    Make no actions if the provider is never used $e1
    Manage writer lifecycle after a writer is opened $e2
    Manage writer lifecycle after an exception using the writer $e3
    Retry opening a writer and send alerts when there is a setup exception opening the writer $e4
    Retry opening a writer if there is a transient exception opening the writer, with limited number of attempts and no monitoring alerts $e5
    Retry setup error according to a single backoff policy when multiple concurrent fibers want to build a writer $e6
    Become healthy after recovering from an earlier setup error $e7
    Become healthy after recovering from an earlier transient error $e8
  """

  def e1 = control.flatMap { c =>
    val io = Writer.provider(c.writerBuilder, retriesConfig, c.appHealth, c.monitoring).use_

    for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(Vector()),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e2 = control.flatMap { c =>
    val io = Writer.provider(c.writerBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.ClosedWriter
    )

    for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e3 = control.flatMap { c =>
    val io = Writer.provider(c.writerBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use { _ =>
        goBOOM
      }
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.ClosedWriter
    )

    for {
      _ <- io.voidError
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e4 = control.flatMap { c =>
    // An writer builder that throws an exception when trying to build a writer
    val throwingBuilder = new Writer.Builder[IO] {
      def build: IO[Writer.CloseableWriter[IO]] =
        c.writerBuilder.build *> raiseForSetupError
    }

    val io = Writer.provider(throwingBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.SentAlert(0L),
      Action.OpenedWriter,
      Action.SentAlert(30L),
      Action.OpenedWriter,
      Action.SentAlert(90L),
      Action.OpenedWriter,
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      _ <- fiber.cancel
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beUnhealthy
    ).reduce(_ and _)

    TestControl.executeEmbed(test)
  }

  def e5 = control.flatMap { c =>
    // An writer builder that throws an exception when trying to build a writer
    val throwingBuilder = new Writer.Builder[IO] {
      def build: IO[Writer.CloseableWriter[IO]] =
        c.writerBuilder.build *> goBOOM
    }

    val io = Writer.provider(throwingBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.OpenedWriter,
      Action.OpenedWriter,
      Action.OpenedWriter,
      Action.OpenedWriter
    )

    val test = for {
      _ <- io.voidError
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beUnhealthy
    ).reduce(_ and _)

    TestControl.executeEmbed(test)
  }

  def e6 = control.flatMap { c =>
    // An builder that throws an exception when trying to build a writer
    val throwingBuilder = new Writer.Builder[IO] {
      def build: IO[Writer.CloseableWriter[IO]] =
        c.writerBuilder.build *> raiseForSetupError
    }

    // Three concurrent fibers wanting to build the writer:
    val io = Writer.provider(throwingBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      Supervisor[IO](await = false).use { supervisor =>
        supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          IO.never
      }
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.SentAlert(0L),
      Action.OpenedWriter,
      Action.SentAlert(30L),
      Action.OpenedWriter,
      Action.SentAlert(90L),
      Action.OpenedWriter,
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      state <- c.state.get
      health <- c.appHealth.status
      _ <- fiber.cancel
    } yield List(
      state should beEqualTo(expectedState),
      health should beUnhealthy
    ).reduce(_ and _)
    TestControl.executeEmbed(test)
  }

  def e7 = control.flatMap { c =>
    // An writer builder that throws an exception *once* and is healthy thereafter
    val throwingOnceBuilder = Ref[IO].of(false).map { hasThrownException =>
      new Writer.Builder[IO] {
        def build: IO[Writer.CloseableWriter[IO]] =
          hasThrownException.get.flatMap {
            case false =>
              hasThrownException.set(true) *> c.writerBuilder.build *> raiseForSetupError
            case true =>
              c.writerBuilder.build
          }
      }
    }

    val io = throwingOnceBuilder.flatMap { writerBuilder =>
      Writer.provider(writerBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
        provider.opened.use_
      }
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.SentAlert(0L),
      Action.OpenedWriter,
      Action.ClosedWriter
    )

    val test = for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
    TestControl.executeEmbed(test)
  }

  def e8 = control.flatMap { c =>
    // An writer builder that throws an exception *once* and is healthy thereafter
    val throwingOnceBuilder = Ref[IO].of(false).map { hasThrownException =>
      new Writer.Builder[IO] {
        def build: IO[Writer.CloseableWriter[IO]] =
          hasThrownException.get.flatMap {
            case false =>
              hasThrownException.set(true) *> c.writerBuilder.build *> goBOOM
            case true =>
              c.writerBuilder.build
          }
      }
    }

    val io = throwingOnceBuilder.flatMap { writerBuilder =>
      Writer.provider(writerBuilder, retriesConfig, c.appHealth, c.monitoring).use { provider =>
        provider.opened.use_
      }
    }

    val expectedState = Vector(
      Action.OpenedWriter,
      Action.OpenedWriter,
      Action.ClosedWriter
    )

    val test = for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
    TestControl.executeEmbed(test)
  }

  /** Convenience matchers for health probe * */

  def beHealthy: org.specs2.matcher.Matcher[HealthProbe.Status] = { (status: HealthProbe.Status) =>
    val result = status match {
      case HealthProbe.Healthy      => true
      case HealthProbe.Unhealthy(_) => false
    }
    (result, s"$status is not healthy")
  }

  def beUnhealthy: org.specs2.matcher.Matcher[HealthProbe.Status] = { (status: HealthProbe.Status) =>
    val result = status match {
      case HealthProbe.Healthy      => false
      case HealthProbe.Unhealthy(_) => true
    }
    (result, s"$status is not unhealthy")
  }
}

object WriterProviderSpec {

  sealed trait Action

  object Action {
    case object OpenedWriter extends Action
    case object ClosedWriter extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
  }

  case class Control(
    state: Ref[IO, Vector[Action]],
    writerBuilder: Writer.Builder[IO],
    appHealth: AppHealth[IO],
    monitoring: Monitoring[IO]
  )

  def retriesConfig = Config.Retries(
    Config.SetupErrorRetries(30.seconds),
    Config.TransientErrorRetries(1.second, 5),
    Config.AlterTableWaitRetries(1.second)
  )

  def control: IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      appHealth <- testAppHealth()
    } yield Control(state, testWriterBuilder(state), appHealth, testMonitoring(state))

  private def testAppHealth(): IO[AppHealth[IO]] = {
    val everythingHealthy: Map[AppHealth.Service, Boolean] = Map(Service.BigQueryClient -> true, Service.BadSink -> true)
    val healthySource = new SourceAndAck[IO] {
      override def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): fs2.Stream[IO, Nothing] =
        fs2.Stream.empty

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO(SourceAndAck.Healthy)
    }
    AppHealth.init(10.seconds, healthySource, everythingHealthy)
  }

  private def testWriterBuilder(state: Ref[IO, Vector[Action]]): Writer.Builder[IO] =
    new Writer.Builder[IO] {
      def build: IO[Writer.CloseableWriter[IO]] =
        state.update(_ :+ Action.OpenedWriter).as(testCloseableWriter(state))
    }

  private def testCloseableWriter(state: Ref[IO, Vector[Action]]): Writer.CloseableWriter[IO] = new Writer.CloseableWriter[IO] {
    def descriptor: IO[Descriptors.Descriptor] =
      IO(AtomicDescriptor.get)

    def write(rows: List[Map[String, AnyRef]]): IO[Writer.WriteResult] = IO.pure(Writer.WriteResult.Success)

    def close: IO[Unit] = state.update(_ :+ Action.ClosedWriter)
  }

  private def testMonitoring(state: Ref[IO, Vector[Action]]): Monitoring[IO] = new Monitoring[IO] {
    def alert(message: Alert): IO[Unit] =
      for {
        now <- IO.realTime
        _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
      } yield ()
  }

  // Raise an exception in an IO
  def goBOOM[A]: IO[A] = IO.raiseError(new RuntimeException("boom!")).adaptError { t =>
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

  // Raise a known exception that indicates a problem with the warehouse setup
  def raiseForSetupError[A]: IO[A] = IO.raiseError {
    val inner = new RuntimeException("go away")
    inner.setStackTrace(Array()) // don't clutter our test logs
    val t = new PermissionDeniedException(inner, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false)
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

}
