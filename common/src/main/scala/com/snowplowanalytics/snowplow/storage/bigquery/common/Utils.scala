package com.snowplowanalytics.snowplow.storage.bigquery.common

import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{ compact, fromJsonNode }

import scalaz.{Failure, Success, ValidationNel}

import io.circe.Json
import io.circe.parser.parse

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.either._

import com.fasterxml.jackson.databind.JsonNode

object Utils {
  def toCirce(jvalue: JValue): Json =
    parse(compact(jvalue)).getOrElse(throw new RuntimeException(s"Unexpected JValue [$jvalue]"))

  def fromJackson(json: JsonNode): Json =
    toCirce(fromJsonNode(json))

  def fromValidation[E, A](validation: ValidationNel[E, A]): Either[NonEmptyList[E], A] =
    validation match {
      case Success(a) => a.asRight
      case Failure(errors) => NonEmptyList.fromListUnsafe(errors.list).asLeft
    }

  def fromValidationZ[E, A](validation: ValidationNel[E, A]): ValidatedNel[E, A] =
    fromValidation(validation).toValidated

  def catchNonFatalMessage[A](a: => A): ValidatedNel[String, A] =
    Validated
      .catchNonFatal(a)
      .leftMap(_.getMessage)
      .toValidatedNel
}
