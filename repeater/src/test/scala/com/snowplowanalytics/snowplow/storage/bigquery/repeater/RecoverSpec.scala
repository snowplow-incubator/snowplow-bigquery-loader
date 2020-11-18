package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import scala.io.Source

import io.circe.Json
import io.circe.parser.parse

import org.specs2.Specification

class RecoverSpec extends Specification {

  def is =
    s2"""
      recover parses json   $e1
    """

  def e1 = {
    val in        = Source.fromResource("failed_inserts.json").mkString
    val recovered = Recover.recover(in)
    val out       = parse(Source.fromResource("payload_fixed.json").mkString).getOrElse(Json.Null)

    recovered must beRight.like {
      case Recover.IdAndEvent(_, event) => event must beEqualTo(out)
    }
  }
}
