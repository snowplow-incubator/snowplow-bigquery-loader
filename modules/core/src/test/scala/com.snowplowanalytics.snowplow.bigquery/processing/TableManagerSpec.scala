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
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import com.google.api.gax.rpc.PermissionDeniedException
import com.google.api.gax.grpc.GrpcStatusCode
import io.grpc.Status
import com.google.cloud.bigquery.{BigQueryError, BigQueryException}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.bigquery.{Alert, AppHealth, Config, Monitoring}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.bigquery.AppHealth.Service
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck}

class TableManagerSpec extends Specification with CatsEffect {
  import TableManagerSpec._

  def is = s2"""
  The table manager
    Add columns when told to $e1
    Retry adding columns and send alerts when there is a setup exceptionr $e2
    Retry adding columns if there is a transient exception, with limited number of attempts and no monitoring alerts $e3
    Become healthy after recovering from an earlier setup error $e4
    Become healthy after recovering from an earlier transient error $e5
    Disable future attempts to add columns if the table has too many columns $e6
    Eventually re-enable future attempts to add columns if the table has too many columns $e7
  """

  def e1 = control().flatMap { c =>
    val testFields1 = Vector(
      Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
      Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
    )

    val testFields2 = Vector(
      Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
      Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
    )

    val io = for {
      tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
      _ <- tableManager.addColumns(testFields1)
      _ <- tableManager.addColumns(testFields2)
    } yield ()

    val expected = Vector(
      Action.AddColumnsAttempted(testFields1),
      Action.AddColumnsAttempted(testFields2)
    )

    for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status
    } yield List(
      state should beEqualTo(expected),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e2 = {
    val mocks = List.fill(100)(Response.ExceptionThrown(testSetupError))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.SentAlert(0L),
        Action.AddColumnsAttempted(testFields),
        Action.SentAlert(30L),
        Action.AddColumnsAttempted(testFields),
        Action.SentAlert(90L),
        Action.AddColumnsAttempted(testFields),
        Action.SentAlert(210L)
      )

      val test = for {
        fiber <- io.start
        _ <- IO.sleep(4.minutes)
        _ <- fiber.cancel
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beUnhealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
  }

  def e3 = {
    val mocks = List.fill(100)(Response.ExceptionThrown(new RuntimeException("Boom!")))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.AddColumnsAttempted(testFields),
        Action.AddColumnsAttempted(testFields),
        Action.AddColumnsAttempted(testFields),
        Action.AddColumnsAttempted(testFields)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beUnhealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
  }

  def e4 = {
    val mocks = List(Response.ExceptionThrown(testSetupError))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.SentAlert(0L),
        Action.AddColumnsAttempted(testFields)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beHealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
  }

  def e5 = {
    val mocks = List(Response.ExceptionThrown(new RuntimeException("boom!")))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.AddColumnsAttempted(testFields)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beHealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
  }

  def e6 = {
    val error     = new BigQueryError("invalid", "global", "Too many columns (10067) in schema. Only 10000 columns are allowed.")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(10.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.SentAlert(0L)
      )

      val test = for {
        _ <- io
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beHealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
  }

  def e7 = {
    val error     = new BigQueryError("invalid", "global", "Too many columns (10067) in schema. Only 10000 columns are allowed.")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth, c.monitoring)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(310.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.SentAlert(0L),
        Action.AddColumnsAttempted(testFields2)
      )

      val test = for {
        _ <- io
        state <- c.state.get
        health <- c.appHealth.status
      } yield List(
        state should beEqualTo(expected),
        health should beHealthy
      ).reduce(_ and _)

      TestControl.executeEmbed(test)
    }
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

object TableManagerSpec {

  sealed trait Action

  object Action {
    case class AddColumnsAttempted(columns: Vector[Field]) extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
  }

  sealed trait Response
  object Response {
    case object Success extends Response
    final case class ExceptionThrown(value: Throwable) extends Response
  }

  case class Mocks(addColumnsResults: List[Response])

  case class Control(
    state: Ref[IO, Vector[Action]],
    tableManager: TableManager[IO],
    appHealth: AppHealth[IO],
    monitoring: Monitoring[IO]
  )

  def retriesConfig = Config.Retries(
    Config.SetupErrorRetries(30.seconds),
    Config.TransientErrorRetries(1.second, 5),
    Config.AlterTableWaitRetries(1.second),
    Config.TooManyColumnsRetries(300.seconds)
  )

  def control(mocks: Mocks = Mocks(Nil)): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      appHealth <- testAppHealth()
      tableManager <- testTableManager(state, mocks.addColumnsResults)
    } yield Control(state, tableManager, appHealth, testMonitoring(state))

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

  private def testTableManager(state: Ref[IO, Vector[Action]], mocks: List[Response]): IO[TableManager[IO]] =
    for {
      mocksRef <- Ref[IO].of(mocks)
    } yield new TableManager[IO] {
      def addColumns(columns: Vector[Field]): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.AddColumnsAttempted(columns))
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def createTable: IO[Unit] =
        IO.unit

    }

  private def testMonitoring(state: Ref[IO, Vector[Action]]): Monitoring[IO] = new Monitoring[IO] {
    def alert(message: Alert): IO[Unit] =
      for {
        now <- IO.realTime
        _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
      } yield ()
  }

  def testSetupError: Throwable = {
    val inner = new RuntimeException("go away")
    inner.setStackTrace(Array()) // don't clutter our test logs
    val t = new PermissionDeniedException(inner, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false)
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

}
