/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.IO
import fs2.{Chunk, Stream}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import io.circe.literal._
import com.google.cloud.bigquery.{Field => BQField, FieldList, StandardSQLTypeName}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent, unstructEventDecoder}
import com.snowplowanalytics.snowplow.bigquery.{AtomicDescriptor, MockEnvironment, RuntimeService}
import com.snowplowanalytics.snowplow.bigquery.MockEnvironment.{Action, Mocks}
import com.snowplowanalytics.snowplow.sources.TokenedEvents

import scala.concurrent.duration.DurationLong

import java.time.Instant
import java.util.UUID

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The bigquery loader should:
    Insert events to Bigquery and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - unstruct $alter1
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - unstruct with legacy columns $alter1_legacy
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - contexts $alter2
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - contexts $alter2_legacy
    Alter the Bigquery table when the writer's protobuf Descriptor has missing nested fields - unstruct $alter3
    Alter the Bigquery table when the writer's protobuf Descriptor has missing nested fields - contexts $alter4
    Skip altering the table when the writer's protobuf Descriptor has relevant self-describing entitiy columns $e5
    Emit BadRows when the WriterProvider reports a problem with the data $e6
    Recover when the WriterProvider reports a server-side schema mismatch $e7
    Recover when the WriterProvider reports the write was closed by an earlier error  $e8
    Mark app as unhealthy when sinking badrows fails $e9
    Mark app as unhealthy when writing to the Writer fails with runtime exception $e10
    Emit BadRows for an unknown schema, if exitOnMissingIgluSchema is false $e11 $e11Legacy
    Crash and exit for an unknown schema, if exitOnMissingIgluSchema is true $e12 $e12Legacy
    Use legacy columns for all fields when legacyColumnMode is enabled $e13
  """

  def e1 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")

    val io = runTest(inputEvents(count = 2, good(optCollectorTstamp = Option(collectorTstamp)))) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(4),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e2 =
    runTest(inputEvents(count = 3, badlyFormatted)) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(0),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }

  def e3 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:52.123Z")

    val toInputs = for {
      bads <- inputEvents(count = 3, badlyFormatted)
      goods <- inputEvents(count = 3, good(optCollectorTstamp = Option(collectorTstamp)))
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack)
    }
    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(6),
          Action.SetE2ELatencyMetric(52123.millis),
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(6),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def alter1_base(
    legacyColumns: Boolean,
    timeout: Boolean
  ) = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:01.123Z")

    val unstructEvent: UnstructEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.snowplow.media/ad_click_event/jsonschema/1-0-0",
        "data": {
          "percentProgress": 50
        }
      }
    }
    """.as[UnstructEvent].fold(throw _, identity)
    val expectedColumnName =
      if (legacyColumns) "unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1_0_0"
      else "unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1"
    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField.of(
            expectedColumnName,
            StandardSQLTypeName.STRUCT,
            FieldList.of(BQField.of("percent_progress", StandardSQLTypeName.STRING))
          )
        )
      ),
      descriptors = List(
        AtomicDescriptor.withWebPage,
        AtomicDescriptor.withWebPage,
        AtomicDescriptor.withWebPageAndAdClick
      )
    )

    val legacyCriteria =
      if (legacyColumns) List(SchemaCriterion("com.snowplowanalytics.snowplow.media", "ad_click_event", "jsonschema", 1))
      else Nil

    val io = runTest(inputEvents(count = 1, good(unstructEvent, optCollectorTstamp = Option(collectorTstamp))), mocks, legacyCriteria) {
      case (inputs, control) =>
        for {
          _ <- IO.sleep(processTime.toEpochMilli.millis)
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
        } yield state.actions should beEqualTo(
          Vector(
            Action.CreatedTable,
            Action.OpenedWriter,
            Action.AlterTableAddedColumns(Vector(expectedColumnName)),
            Action.ClosedWriter,
            Action.OpenedWriter,
            Action.WroteRowsToBigQuery(2),
            Action.SetE2ELatencyMetric(2123.millis),
            Action.AddedGoodCountMetric(2),
            Action.AddedBadCountMetric(0),
            Action.Checkpointed(List(inputs(0).ack))
          )
        )
    }
    if (timeout) TestControl.executeEmbed(io.timeout(10.seconds))
    else TestControl.executeEmbed(io)
  }

  def alter1        = alter1_base(legacyColumns = false, timeout = false)
  def alter1_legacy = alter1_base(legacyColumns = true, timeout = true)

  def alter2_base(
    legacyColumns: Boolean,
    timeout: Boolean
  ) = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")

    val data = json"""{ "percentProgress": 50 }"""
    val contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow.media/ad_click_event/jsonschema/1-0-0").toOption.get,
          data
        )
      )
    )
    val expectedColumnName =
      if (legacyColumns) "contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1_0_0"
      else "contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1"

    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField
            .newBuilder(
              expectedColumnName,
              StandardSQLTypeName.STRUCT,
              FieldList.of(BQField.of("percent_progress", StandardSQLTypeName.STRING))
            )
            .setMode(BQField.Mode.REPEATED)
            .build()
        )
      ),
      descriptors = List(
        AtomicDescriptor.initial,
        AtomicDescriptor.initial,
        AtomicDescriptor.withAdClickContext
      )
    )

    val legacyCriteria =
      if (legacyColumns) List(SchemaCriterion("com.snowplowanalytics.snowplow.media", "ad_click_event", "jsonschema", 1))
      else Nil

    val io =
      runTest(inputEvents(count = 1, good(contexts = contexts, optCollectorTstamp = Option(collectorTstamp))), mocks, legacyCriteria) {
        case (inputs, control) =>
          for {
            _ <- IO.sleep(processTime.toEpochMilli.millis)
            _ <- Processing.stream(control.environment).compile.drain
            state <- control.state.get
          } yield state.actions should beEqualTo(
            Vector(
              Action.CreatedTable,
              Action.OpenedWriter,
              Action.AlterTableAddedColumns(Vector(expectedColumnName)),
              Action.ClosedWriter,
              Action.OpenedWriter,
              Action.WroteRowsToBigQuery(2),
              Action.SetE2ELatencyMetric(43123.millis),
              Action.AddedGoodCountMetric(2),
              Action.AddedBadCountMetric(0),
              Action.Checkpointed(List(inputs(0).ack))
            )
          )
      }
    if (timeout) TestControl.executeEmbed(io.timeout(10.seconds))
    else TestControl.executeEmbed(io)
  }

  def alter2        = alter2_base(legacyColumns = false, timeout = false)
  def alter2_legacy = alter2_base(legacyColumns = true, timeout = true)

  def alter3 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")

    val data = json"""{ "myInteger": 100 }"""
    val unstruct = UnstructEvent(
      Some(SelfDescribingData(SchemaKey.fromUri("iglu:test_vendor/test_name/jsonschema/1-0-1").toOption.get, data))
    )

    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField.of(
            "unstruct_event_test_vendor_test_name_1",
            StandardSQLTypeName.STRUCT,
            FieldList.of(
              BQField.of("my_string", StandardSQLTypeName.STRING),
              BQField.of("my_integer", StandardSQLTypeName.INT64)
            )
          )
        )
      ),
      descriptors = List(
        AtomicDescriptor.withTestUnstruct100,
        AtomicDescriptor.withTestUnstruct100,
        AtomicDescriptor.withTestUnstruct101
      )
    )
    val io = runTest(inputEvents(count = 1, good(ue = unstruct, optCollectorTstamp = Option(collectorTstamp))), mocks) {
      case (inputs, control) =>
        for {
          _ <- IO.sleep(processTime.toEpochMilli.millis)
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
        } yield state.actions should beEqualTo(
          Vector(
            Action.CreatedTable,
            Action.OpenedWriter,
            Action.AlterTableAddedColumns(Vector("unstruct_event_test_vendor_test_name_1")),
            Action.ClosedWriter,
            Action.OpenedWriter,
            Action.WroteRowsToBigQuery(2),
            Action.SetE2ELatencyMetric(43123.millis),
            Action.AddedGoodCountMetric(2),
            Action.AddedBadCountMetric(0),
            Action.Checkpointed(List(inputs(0).ack))
          )
        )
    }
    TestControl.executeEmbed(io)
  }

  def alter4 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")

    val data     = json"""{ "myInteger": 100}"""
    val contexts = Contexts(List(SelfDescribingData(SchemaKey.fromUri("iglu:test_vendor/test_name/jsonschema/1-0-1").toOption.get, data)))

    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField
            .newBuilder(
              "contexts_test_vendor_test_name_1",
              StandardSQLTypeName.STRUCT,
              FieldList.of(
                BQField.of("my_string", StandardSQLTypeName.STRING),
                BQField.of("my_integer", StandardSQLTypeName.INT64)
              )
            )
            .setMode(BQField.Mode.REPEATED)
            .build()
        )
      ),
      descriptors = List(
        AtomicDescriptor.withTestContext100,
        AtomicDescriptor.withTestContext100,
        AtomicDescriptor.withTestContext101
      )
    )
    val io = runTest(inputEvents(count = 1, good(contexts = contexts, optCollectorTstamp = Option(collectorTstamp))), mocks) {
      case (inputs, control) =>
        for {
          _ <- IO.sleep(processTime.toEpochMilli.millis)
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
        } yield state.actions should beEqualTo(
          Vector(
            Action.CreatedTable,
            Action.OpenedWriter,
            Action.AlterTableAddedColumns(Vector("contexts_test_vendor_test_name_1")),
            Action.ClosedWriter,
            Action.OpenedWriter,
            Action.WroteRowsToBigQuery(2),
            Action.SetE2ELatencyMetric(43123.millis),
            Action.AddedGoodCountMetric(2),
            Action.AddedBadCountMetric(0),
            Action.Checkpointed(List(inputs(0).ack))
          )
        )
    }
    TestControl.executeEmbed(io)
  }

  def e5 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:52.123Z")

    val unstructEvent: UnstructEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
        "data": {
          "id": "07212d37-4257-4fdc-aed1-2ab55a3ff1d9"
        }
      }
    }
    """.as[UnstructEvent].fold(throw _, identity)
    val io = runTest(inputEvents(count = 1, good(unstructEvent, optCollectorTstamp = Option(collectorTstamp)))) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.SetE2ELatencyMetric(52123.millis),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e6 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:52.123Z")

    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.SerializationFailures(Map(0 -> "boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(1),
          Action.SetE2ELatencyMetric(52123.millis),
          Action.SentToBad(1),
          Action.AddedGoodCountMetric(1),
          Action.AddedBadCountMetric(1),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e7 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:52.123Z")
    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.ServerSideSchemaMismatch(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.ServerSideSchemaMismatch(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.SetE2ELatencyMetric(54123.millis),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e8 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.WriterWasClosedByEarlierError(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.WriterWasClosedByEarlierError(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e9 = {
    val mocks = Mocks.default.copy(
      badSinkResponse = MockEnvironment.Response.ExceptionThrown(new RuntimeException("Some error when sinking bad data"))
    )

    runTest(inputEvents(count = 1, badlyFormatted), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.BecameUnhealthy(RuntimeService.BadSink)
        )
      )
    }
  }

  def e10 = {
    val mocks = Mocks.default.copy(
      writerResponses = List(MockEnvironment.Response.ExceptionThrown(new RuntimeException("Some error when writing to the Writer")))
    )
    runTest(inputEvents(count = 1, good()), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.BecameUnhealthy(RuntimeService.BigQueryClient)
        )
      )
    }

  }

  def e11Base(legacyColumns: Boolean, timeout: Boolean) = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")

    val unstructEvent: UnstructEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.unkown/unknown_event/jsonschema/1-0-0",
        "data": {
          "abc": 50
        }
      }
    }
    """.as[UnstructEvent].fold(throw _, identity)

    val legacyCriteria =
      if (legacyColumns) List(SchemaCriterion("com.unknown", "unknown_event", "jsonschema", 1))
      else Nil

    val io =
      runTest(inputEvents(count = 1, good(unstructEvent, optCollectorTstamp = Option(collectorTstamp))), legacyCriteria = legacyCriteria) {
        case (inputs, control) =>
          for {
            _ <- IO.sleep(processTime.toEpochMilli.millis)
            _ <- Processing.stream(control.environment).compile.drain
            state <- control.state.get
          } yield state.actions should beEqualTo(
            Vector(
              Action.CreatedTable,
              Action.OpenedWriter,
              Action.WroteRowsToBigQuery(1),
              Action.SetE2ELatencyMetric(42123.millis),
              Action.SentToBad(1),
              Action.AddedGoodCountMetric(1),
              Action.AddedBadCountMetric(1),
              Action.Checkpointed(List(inputs(0).ack))
            )
          )
      }
    if (timeout) TestControl.executeEmbed(io.timeout(10.seconds))
    else TestControl.executeEmbed(io)
  }

  def e11       = e11Base(legacyColumns = false, timeout = false)
  def e11Legacy = e11Base(legacyColumns = true, timeout = true)

  def e12Base(legacyColumns: Boolean) = {
    val unstructEvent: UnstructEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.unkown/unknown_event/jsonschema/1-0-0",
        "data": {
          "abc": 50
        }
      }
    }
    """.as[UnstructEvent].fold(throw _, identity)

    val legacyCriteria =
      if (legacyColumns) List(SchemaCriterion("com.unknown", "unknown_event", "jsonschema", 1))
      else Nil

    runTest(inputEvents(count = 1, good(unstructEvent)), legacyCriteria = legacyCriteria) { case (_, control) =>
      val environment = control.environment.copy(exitOnMissingIgluSchema = true)
      for {
        _ <- Processing.stream(environment).compile.drain.voidError
        state <- control.state.get
      } yield state.actions should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.BecameUnhealthy(RuntimeService.Iglu)
        )
      )
    }
  }

  def e12       = e12Base(legacyColumns = false)
  def e12Legacy = e12Base(legacyColumns = true)

  def e13 = {
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val eventID    = UUID.randomUUID()
    val vCollector = "1.0.0"
    val vEtl       = "2.0.0"
    val source = goodOne(
      optEventId    = Option(IO(eventID)),
      optVCollector = Option(vCollector),
      optVEtl       = Option(vEtl)
    )

    val io = runTest(inputEvents(count = 1, source), legacyColumnMode = true) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield {
        val rows = state.writtenToBQ
        (rows.size shouldEqual inputs.head.events.size) and
          (rows.head.get("event_id") should beEqualTo(Option(eventID.toString))) and
          (rows.head.get("v_collector") should beEqualTo(Option(vCollector))) and
          (rows.head.get("v_etl") should beEqualTo(Option(vEtl)))
      }
    }
    TestControl.executeEmbed(io)
  }
}

object ProcessingSpec {

  def runTest[A](
    toInputs: IO[List[TokenedEvents]],
    mocks: Mocks                          = Mocks.default,
    legacyCriteria: List[SchemaCriterion] = Nil,
    legacyColumnMode: Boolean             = false
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    toInputs.flatMap { inputs =>
      MockEnvironment.build(inputs, mocks, legacyCriteria, legacyColumnMode).use { control =>
        f(inputs, control)
      }
    }

  def inputEvents(count: Long, source: IO[TokenedEvents]): IO[List[TokenedEvents]] =
    Stream
      .eval(source)
      .repeat
      .take(count)
      .compile
      .toList

  def goodOne(
    ue: UnstructEvent                       = UnstructEvent(None),
    contexts: Contexts                      = Contexts(List.empty),
    optEventId: Option[IO[UUID]]            = None,
    optCollectorTstamp: Option[IO[Instant]] = None,
    optVCollector: Option[String]           = None,
    optVEtl: Option[String]                 = None
  ): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      eventId <- optEventId.fold(IO.randomUUID)(identity)
      collectorTstamp <- optCollectorTstamp.fold(IO.realTimeInstant)(identity)
      vCollector = optVCollector.fold("0.0.0")(identity)
      vEtl       = optVEtl.fold("0.0.0")(identity)
    } yield {
      val e = Event.minimal(eventId, collectorTstamp, vCollector, vEtl).copy(unstruct_event = ue).copy(contexts = contexts)
      TokenedEvents(Chunk(ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))), ack)
    }

  def good(
    ue: UnstructEvent                   = UnstructEvent(None),
    contexts: Contexts                  = Contexts(List.empty),
    optCollectorTstamp: Option[Instant] = None
  ): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      now <- IO.realTimeInstant
      collectorTstamp = optCollectorTstamp.fold(now)(identity)
    } yield {
      val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0").copy(unstruct_event = ue).copy(contexts = contexts)
      val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
      val serialized = Chunk(event1, event2).map { e =>
        ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
      }
      TokenedEvents(serialized, ack)
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }
}
