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

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{UnstructEvent, unstructEventDecoder}
import com.snowplowanalytics.snowplow.bigquery.MockEnvironment
import com.snowplowanalytics.snowplow.bigquery.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The bigquery loader should:
    Insert events to Bigquery and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Alter the Bigquery table when the writer's protobuf Descriptor has missing columns $e4
    Emit BadRows when the WriterProvider reports a problem with the data $e5
    Set the latency metric based off the message timestamp $e6
  """

  def e1 =
    for {
      inputs <- generateEvents().take(2).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.OpenedWriter,
        Action.WroteRowsToBigQuery(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack)),
        Action.ClosedWriter
      )
    )

  def e2 =
    for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.OpenedWriter,
        Action.SentToBad(6),
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(6),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack)),
        Action.ClosedWriter
      )
    )

  def e3 =
    for {
      bads <- generateBadlyFormatted.take(3).compile.toList
      goods <- generateEvents().take(3).compile.toList
      inputs = bads.zip(goods).map { case (bad, good) =>
                 TokenedEvents(bad.events ++ good.events, good.ack, None)
               }
      control <- MockEnvironment.build(inputs)
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
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack)),
        Action.ClosedWriter
      )
    )

  def e4 = {
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

    for {
      inputs <- generateEvents(unstructEvent).take(1).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.OpenedWriter,
        Action.AlterTableAddedColumns(List("unstruct_event_com_snowplowanalytics_snowplow_web_page_1")),
        Action.ClosedWriter,
        Action.OpenedWriter,
        Action.WroteRowsToBigQuery(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack)),
        Action.ClosedWriter
      )
    )
  }

  def e5 = {
    val mockedChannelResponses = List(
      WriterProvider.WriteResult.SerializationFailures(Map(0 -> "boom!")),
      WriterProvider.WriteResult.Success
    )

    for {
      inputs <- generateEvents().take(1).compile.toList
      control <- MockEnvironment.build(inputs, mockedChannelResponses)
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
        Action.Checkpointed(List(inputs(0).ack)),
        Action.ClosedWriter
      )
    )
  }

  def e6 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val io = for {
      inputs <- generateEvents().take(2).compile.toList.map {
                  _.map {
                    _.copy(earliestSourceTstamp = Some(messageTime))
                  }
                }
      control <- MockEnvironment.build(inputs)
      _ <- IO.sleep(processTime.toEpochMilli.millis)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SetLatencyMetric(42123),
        Action.SetLatencyMetric(42123),
        Action.OpenedWriter,
        Action.WroteRowsToBigQuery(4),
        Action.AddedGoodCountMetric(4),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack)),
        Action.ClosedWriter
      )
    )

    TestControl.executeEmbed(io)

  }

}

object ProcessingSpec {

  def generateEvents(ue: UnstructEvent = UnstructEvent(None)): Stream[IO, TokenedEvents] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0").copy(unstruct_event = ue)
        val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
        val serialized = Chunk(event1, event2).map { e =>
          ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
        }
        TokenedEvents(serialized, ack, None)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        TokenedEvents(serialized, token, None)
      }
    }.repeat

}
