/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.Async
import cats.implicits._
import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures}
import com.google.common.util.concurrent.MoreExecutors

object FutureInterop {
  def fromFuture[F[_]: Async, A](future: ApiFuture[A]): F[Unit] =
    Async[F]
      .async[A] { cb =>
        val cancel = Async[F].delay {
          future.cancel(false)
        }.void
        Async[F].delay {
          addCallback(future, cb)
          Some(cancel)
        }
      }
      .void

  private def addCallback[A](fut: ApiFuture[A], cb: Either[Throwable, A] => Unit): Unit = {
    val apiFutureCallback = new ApiFutureCallback[A] {
      def onFailure(t: Throwable): Unit = cb(Left(t))
      def onSuccess(result: A): Unit    = cb(Right(result))
    }
    ApiFutures.addCallback(fut, apiFutureCallback, MoreExecutors.directExecutor)
  }

}
