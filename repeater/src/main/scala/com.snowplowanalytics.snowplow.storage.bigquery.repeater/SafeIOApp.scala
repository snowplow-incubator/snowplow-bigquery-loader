package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import cats.effect.{IOApp, Resource, SyncIO}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

trait SafeIOApp extends IOApp.WithContext {

  implicit val ec: ExecutionContext =
    scala.concurrent.ExecutionContext.Implicits.global

  private val log: Logger = LoggerFactory.getLogger(Repeater.getClass)

  final val exitingEC: ExecutionContext = new ExecutionContext {
    def execute(r: Runnable): Unit =
      ec.execute { () =>
        try r.run()
        catch {
          case t: Throwable =>
            log.error("Unhandled error occurred... Shutting down", t)
            System.exit(-1)
        }
      }

    def reportFailure(cause: Throwable): Unit =
      ec.reportFailure(cause)
  }

  val executionContextResource: Resource[SyncIO, ExecutionContext] =
    Resource.liftF(SyncIO(exitingEC))
}