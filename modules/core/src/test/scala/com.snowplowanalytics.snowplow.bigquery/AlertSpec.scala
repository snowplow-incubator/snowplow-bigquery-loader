/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import org.specs2.Specification

class AlertSpec extends Specification {

  def is = s2"""
  An Alert should:
    Generate a message describing an exception and any nested cause $e1
    Collapse messages if a cause's message contains the parent exception's message $e2
    Collapse messages if a parent exception's message contains the cause's message $e3
  """

  def e1 = {
    val e1 = new RuntimeException("original cause")
    val e2 = new RuntimeException("middle cause", e1)
    val e3 = new RuntimeException("final error", e2)

    val alert = Alert.FailedToCreateEventsTable(e3)

    val expected = "Failed to create events table: final error: middle cause: original cause"

    Alert.getMessage(alert) must beEqualTo(expected)
  }

  def e2 = {
    val e1 = new RuntimeException("This happened: original cause")
    val e2 = new RuntimeException("original cause", e1)

    val alert = Alert.FailedToCreateEventsTable(e2)

    val expected = "Failed to create events table: This happened: original cause"

    Alert.getMessage(alert) must beEqualTo(expected)
  }

  def e3 = {
    val e1 = new RuntimeException("original cause")
    val e2 = new RuntimeException("This happened: original cause", e1)

    val alert = Alert.FailedToCreateEventsTable(e2)

    val expected = "Failed to create events table: This happened: original cause"

    Alert.getMessage(alert) must beEqualTo(expected)
  }
}
