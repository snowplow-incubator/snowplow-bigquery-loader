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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import org.joda.time.Duration

import scala.collection.convert.decorateAsScala._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

import org.apache.beam.model.pipeline.v1.RunnerApi.StateSpec
import org.apache.beam.sdk.state.{ Timer, ValueState, TimeDomain, TimerSpecs }
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ ProcessElement, StateId, OnTimer, StateId, TimerId }
import org.apache.beam.sdk.transforms.windowing.BoundedWindow


/**
  * Aggregate function designed to reduce `types`-topic load
  *
  */
class TypesAggregator extends DoFn[Set[InventoryItem], Set[InventoryItem]] {

  private val FlushPeriod = Duration.standardSeconds(30)

  @TimerId("expiry")
  private val expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

  @ProcessElement
  def process(context: ProcessContext,
              @StateId("buffer") state: ValueState[Set[InventoryItem]],
              @TimerId("expiry") expiryTimer: Timer): Unit = {

    val current = context.element()
    val accumulator = state.read()

    val hasNew = !current.forall(elem => accumulator.contains(elem))
    if (hasNew) {
      state.write()

    }
    if (hasNew && accumulator.isEmpty) {
      expiryTimer.offset(FlushPeriod).setRelative()
    } else if (hasNew) {


    }


    val contains = state.contains(element).read()
    if (!contains) {
      if (!state.read().iterator().hasNext) {
      }

      context.output(Set(element))
    }
  }

  @OnTimer("expiry")
  def onExpiry(context: OnTimerContext,
               @StateId("buffer") bufferState: SetState[InventoryItem]): Unit = {
    if (!bufferState.isEmpty.read) {
      context.output(bufferState.read().asScala.toSet)
    }
  }
}
