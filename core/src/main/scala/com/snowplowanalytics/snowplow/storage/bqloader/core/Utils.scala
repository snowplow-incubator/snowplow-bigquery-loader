package com.snowplowanalytics.snowplow.storage.bqloader.core

import scalaz.{Failure, Success, ValidationNel}

import cats.data.{Validated, ValidatedNel, NonEmptyList}
import cats.syntax.either._

object Utils {
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
