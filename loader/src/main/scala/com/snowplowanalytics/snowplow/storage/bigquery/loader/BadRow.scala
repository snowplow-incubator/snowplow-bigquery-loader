package com.snowplowanalytics.snowplow.storage.bigquery.loader

import java.util.Base64

import cats.data.NonEmptyList
import com.spotify.scio.bigquery.TableRow
import io.circe.{ Json, Encoder }
import io.circe.syntax._

sealed trait BadRow {
  private[loader] def reason: BadRow.Reason
}

object BadRow {

  private[loader] sealed trait Reason extends Product with Serializable
  private[loader] object Reason {
    case object FailedInsert extends Reason
    case object InvalidRow extends Reason

    implicit val encoder: Encoder[Reason] = Encoder.instance {
      case FailedInsert => Json.fromString("FAILED_INSERT")
      case InvalidRow => Json.fromString("INVALID_ROW")
    }
  }

  implicit val badRowEncoder = Encoder.instance[BadRow] {
    case FailedInsert(original, attempt) =>
      Json.fromFields(Map(
        "reason" -> (Reason.FailedInsert: Reason).asJson,
        "original" -> Json.fromString(original.toString),
        "attempt" -> Json.fromInt(attempt)
      ))
  }

  case class FailedInsert(original: TableRow, attempt: Int) extends BadRow {
    private[loader] def reason = Reason.FailedInsert
  }

  case class InvalidRow(original: String, errors: NonEmptyList[String]) extends BadRow {
    private[loader] def reason = Reason.InvalidRow

    def compact: String =
      Json.fromFields(List(
        ("original", Json.fromString(original)),
        ("errors", Json.fromValues(errors.toList.map(Json.fromString)))
      )).noSpaces
  }
}
