package com.snowplowanalytics.snowplow.storage.bqmutator.generator

sealed trait FieldMode extends Product with Serializable

object FieldMode {

  case object Nullable extends FieldMode

  case object Required extends FieldMode

  case object Repeated extends FieldMode

  def get(b: Boolean): FieldMode =
    if (b) Required else Nullable

  def sort(fieldMode: FieldMode): Int =
    fieldMode match {
      case Required => -1
      case Repeated => 0
      case Nullable => 1
    }
}

