package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import org.specs2.Specification
import scala.io.Source

class RecoverSpec extends Specification {

  def is =
    s2"""
      fix wrong column name $e1
      recover parses json   $e2
    """

  def e1 = {
    val in    = Source.fromResource("payload.json").mkString
    val fixed = Recover.fix(in)
    val out   = Source.fromResource("payload_fixed.json").mkString

    fixed ==== out
  }

  def e2 = {
    val in        = Source.fromResource("failed_inserts.json").mkString
    val recovered = Recover.recover(Recover.parseJson(in)).getOrElse("")
    val out       = Source.fromResource("payload_fixed.json").mkString

    def normalize(s: String) = s.replace(" ", "").replace("\n", "")

    normalize(recovered) ==== normalize(out)
  }

}
