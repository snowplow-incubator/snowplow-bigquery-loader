/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import org.json4s.jackson.JsonMethods.compact

import org.joda.time.Duration

import org.apache.beam.sdk.state.{TimeDomain, Timer, TimerSpecs, ValueState, StateSpecs}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.{OnTimer, ProcessElement, StateId, TimerId}

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import common.Codecs.toPayload

/**
  * Aggregate function designed to reduce `types`-topic load
  *
  */
class TypeAggregator extends DoFn[Set[InventoryItem], String] {
  import TypeAggregator._

  // Should it be static
  /** Send all buffered types after period */
  private final val FlushPeriod = Duration.standardSeconds(30)

  @TimerId("expiry")
  private final val ExpirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

  @StateId("buffer")
  private final val BufferState = StateSpecs.value[Set[InventoryItem]]()

  @ProcessElement
  def process(context: ProcessContext,
              @StateId("buffer") state: ValueState[Set[InventoryItem]],
              @TimerId("expiry") expiryTimer: Timer): Unit = {

    val current = context.element()
    val accumulator = state.read()
    val newElements = current -- accumulator

    if (newElements.nonEmpty) {
      if (accumulator.isEmpty) {
        expiryTimer.offset(FlushPeriod).setRelative()
      }
      state.write(accumulator ++ newElements)
      context.output(encode(newElements))
    }
  }

  @OnTimer("expiry")
  def onExpiry(context: OnTimerContext,
               @StateId("buffer") bufferState: ValueState[Set[InventoryItem]]): Unit = {
    if (bufferState.read().nonEmpty) {
      context.output(encode(bufferState.read()))
      bufferState.clear()
    }
  }
}

object TypeAggregator {
  val transformation = ParDo.of(new TypeAggregator)

  def encode(types: Set[InventoryItem]): String =
    compact(toPayload(types))
}
