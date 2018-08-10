package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import org.apache.beam.model.pipeline.v1.RunnerApi.StateSpec
import org.apache.beam.sdk.state.{BagState, SetState, Timer, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.DoFn.TimerId
import scala.collection.convert.decorateAsScala._


import org.apache.beam.sdk.state.BagState
import org.apache.beam.sdk.transforms.DoFn.OnTimer
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.state.TimeDomain
import org.apache.beam.sdk.state.TimerSpecs
import org.apache.beam.sdk.transforms.DoFn.TimerId

import org.joda.time.Duration


/**
  * Aggregate function designed to drastically reduce `types`-topic load
  */
class TypesUpdater extends DoFn[Set[InventoryItem], Set[InventoryItem]] {

  private val MAX_BUFFER_DURATION = Duration.standardSeconds(30)

  @TimerId("expiry")
  private val expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

  @ProcessElement
  def process(context: ProcessContext,
              @StateId("buffer") state: SetState[InventoryItem],
              @TimerId("expiry") expiryTimer: Timer): Unit = {

    val current = context.element()
    val accumulator = state.read().asScala.toSet

    val hasNew = !current.forall(elem => accumulator.contains(elem))
    if (hasNew && accumulator.isEmpty) {
      expiryTimer.offset(MAX_BUFFER_DURATION).setRelative()
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
