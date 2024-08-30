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
import cats.Show
import cats.implicits.showInterpolator

import com.snowplowanalytics.snowplow.runtime.SetupExceptionMessages

sealed trait Alert
object Alert {

  final case class FailedToGetTable(reasons: SetupExceptionMessages) extends Alert
  final case class FailedToCreateEventsTable(reasons: SetupExceptionMessages) extends Alert
  final case class FailedToAddColumns(columns: Vector[String], cause: SetupExceptionMessages) extends Alert
  final case class FailedToOpenBigQueryWriter(reasons: SetupExceptionMessages) extends Alert

  implicit def showAlert: Show[Alert] = Show[Alert] {
    case FailedToGetTable(cause)            => show"Failed to get table: $cause"
    case FailedToCreateEventsTable(cause)   => show"Failed to create table: $cause"
    case FailedToAddColumns(columns, cause) => show"Failed to add columns: ${columns.mkString("[", ",", "]")}. Cause: $cause"
    case FailedToOpenBigQueryWriter(cause)  => show"Failed to open BigQuery writer: $cause"
  }

}
