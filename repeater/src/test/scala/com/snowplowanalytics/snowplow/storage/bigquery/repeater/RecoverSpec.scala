package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import org.specs2.Specification
import scala.io.Source

class RecoverSpec extends Specification {

  def is =
    s2"""
      fix wrong column name $e1
    """

  def e1 = {
    val in        = Source.fromResource("payload.json").mkString
    val recovered = Recover.fix(in)
    val out       = Source.fromResource("payload_fixed.json").mkString

    recovered ==== out
  }

}
