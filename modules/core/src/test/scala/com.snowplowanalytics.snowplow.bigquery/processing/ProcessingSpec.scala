/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
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
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent, unstructEventDecoder}
import com.snowplowanalytics.snowplow.bigquery.{AtomicDescriptor, MockEnvironment}
import com.snowplowanalytics.snowplow.bigquery.MockEnvironment.{Action, Mocks}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The bigquery loader should:
    Insert events to Bigquery and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - unstruct $e4_1
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns - contexts $e4_2
    Alter the Bigquery table when the writer's protobuf Descriptor has missing nested fields - unstruct $e4_3 
    Alter the Bigquery table when the writer's protobuf Descriptor has missing nested fields - contexts $e4_4
    Skip altering the table when the writer's protobuf Descriptor has relevant self-describing entitiy columns $e5
    Emit BadRows when the WriterProvider reports a problem with the data $e6
    Recover when the WriterProvider reports a server-side schema mismatch $e7
    Recover when the WriterProvider reports the write was closed by an earlier error  $e8
    Set the latency metric based off the message timestamp $e9
    Mark app as unhealthy when sinking badrows fails $e10
    Mark app as unhealthy when writing to the Writer fails with runtime exception $e11
  """

  def e1 =
    runTest(inputEvents(count = 2, good())) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(4),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }

  def e2 =
    runTest(inputEvents(count = 3, badlyFormatted)) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
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
    val toInputs = for {
      bads <- inputEvents(count = 3, badlyFormatted)
      goods <- inputEvents(count = 3, good())
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack, None)
    }
    runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(6),
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(6),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }
  }

  def e4_1 = {
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
    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField.of(
            "unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1",
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
    runTest(inputEvents(count = 1, good(unstructEvent)), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.AlterTableAddedColumns(Vector("unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1")),
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e4_2 = {
    val data = json"""{ "percentProgress": 50 }"""
    val contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow.media/ad_click_event/jsonschema/1-0-0").toOption.get,
          data
        )
      )
    )

    val mocks = Mocks.default.copy(
      addColumnsResponse = MockEnvironment.Response.Success(
        FieldList.of(
          BQField
            .newBuilder(
              "contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1",
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
    runTest(inputEvents(count = 1, good(contexts = contexts)), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.AlterTableAddedColumns(Vector("contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1")),
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e4_3 = {
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
    runTest(inputEvents(count = 1, good(ue = unstruct)), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.AlterTableAddedColumns(Vector("unstruct_event_test_vendor_test_name_1")),
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e4_4 = {
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
    runTest(inputEvents(count = 1, good(contexts = contexts)), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.AlterTableAddedColumns(Vector("contexts_test_vendor_test_name_1")),
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e5 = {
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
    runTest(inputEvents(count = 1, good(unstructEvent))) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e6 = {
    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.SerializationFailures(Map(0 -> "boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    runTest(inputEvents(count = 1, good()), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(1),
          Action.SentToBad(1),
          Action.AddedGoodCountMetric(1),
          Action.AddedBadCountMetric(1),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e7 = {
    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.ServerSideSchemaMismatch(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.ServerSideSchemaMismatch(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    val io = runTest(inputEvents(count = 1, good()), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e8 = {
    val mocks = Mocks.default.copy(
      writerResponses = List(
        MockEnvironment.Response.Success(Writer.WriteResult.WriterWasClosedByEarlierError(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.WriterWasClosedByEarlierError(new RuntimeException("boom!"))),
        MockEnvironment.Response.Success(Writer.WriteResult.Success)
      )
    )
    val io = runTest(inputEvents(count = 1, good()), mocks) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.ClosedWriter,
          Action.OpenedWriter,
          Action.WroteRowsToBigQuery(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e9 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val toInputs = inputEvents(count = 2, good()).map {
      _.map {
        _.copy(earliestSourceTstamp = Some(messageTime))
      }
    }

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.CreatedTable,
          Action.OpenedWriter,
          Action.SetLatencyMetric(42123),
          Action.SetLatencyMetric(42123),
          Action.WroteRowsToBigQuery(4),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }

    TestControl.executeEmbed(io)

  }

  def e10 = {
    val mocks = Mocks.default.copy(
      badSinkResponse = MockEnvironment.Response.ExceptionThrown(new RuntimeException("Some error when sinking bad data"))
    )

    runTest(inputEvents(count = 1, badlyFormatted), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        healthStatus <- control.environment.appHealth.status
      } yield healthStatus should beEqualTo(HealthProbe.Unhealthy("Bad sink is not healthy"))
    }
  }

  def e11 = {
    val mocks = Mocks.default.copy(
      writerResponses = List(MockEnvironment.Response.ExceptionThrown(new RuntimeException("Some error when writing to the Writer")))
    )

    runTest(inputEvents(count = 1, good()), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        healthStatus <- control.environment.appHealth.status
      } yield healthStatus should beEqualTo(HealthProbe.Unhealthy("BigQuery client is not healthy"))
    }
  }

}

object ProcessingSpec {

  def runTest[A](
    toInputs: IO[List[TokenedEvents]],
    mocks: Mocks = Mocks.default
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    toInputs.flatMap { inputs =>
      MockEnvironment.build(inputs, mocks).use { control =>
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

  def good(ue: UnstructEvent = UnstructEvent(None), contexts: Contexts = Contexts(List.empty)): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      collectorTstamp <- IO.realTimeInstant
    } yield {
      val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0").copy(unstruct_event = ue).copy(contexts = contexts)
      val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
      val serialized = Chunk(event1, event2).map { e =>
        ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
      }
      TokenedEvents(serialized, ack, None)
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token, None)
    }
}
