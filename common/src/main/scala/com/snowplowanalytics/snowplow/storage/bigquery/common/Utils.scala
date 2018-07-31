package com.snowplowanalytics.snowplow.storage.bigquery.common

import org.json4s.JValue
import org.json4s.jackson.JsonMethods.compact

import scalaz.{Failure, Success, ValidationNel}

import io.circe.Json
import io.circe.parser.parse

import cats.data.{Validated, ValidatedNel, NonEmptyList}
import cats.syntax.either._

object Utils {
  def toCirce(jvalue: JValue): Json =
    parse(compact(jvalue)).getOrElse(throw new RuntimeException(s"Unexpected JValue [$jvalue]"))

  def fromValidation[E, A](validation: ValidationNel[E, A]): Either[NonEmptyList[E], A] =
    validation match {
      case Success(a) => a.asRight
      case Failure(errors) => NonEmptyList.fromListUnsafe(errors.list).asLeft
    }

  def catchNonFatalMessage[A](a: => A): ValidatedNel[String, A] =
    Validated
      .catchNonFatal(a)
      .leftMap(_.getMessage)
      .toValidatedNel
}
