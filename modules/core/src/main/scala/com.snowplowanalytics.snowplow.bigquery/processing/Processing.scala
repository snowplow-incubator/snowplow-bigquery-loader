/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.implicits._
import cats.{Applicative, Foldable}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.{PolicyDecision, RetryDetails, RetryPolicy}
import retry.implicits._

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, NonAtomicFields, SchemaSubVersion, TabledEntity, Transform}
import com.snowplowanalytics.snowplow.bigquery.{AppHealth, Environment, Metrics}

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventProcessingConfig              = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    Stream.eval(env.tableManager.createTable) *>
      Stream.eval(env.writer.opened.use_) *>
      env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParseResult(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    token: Unique.Token
  )

  private case class Batched(
    events: ListOfList[Event],
    parseFailures: ListOfList[BadRow],
    countBytes: Long,
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    tokens: Vector[Unique.Token]
  )

  type EventWithTransform = (Event, Map[String, AnyRef])

  /**
   * State of a batch for all stages post-transform
   *
   * @param toBeInserted
   *   Events from this batch which have not yet been inserted. Events are dropped from this list
   *   once they have either failed or got inserted. Implemented as a Vector because we need to do
   *   lookup by index.
   * @param origBatchSize
   *   The count of events in the original batch. Includes all good and bad events.
   * @param origBatchBytes
   *   The total size in bytes of events in the original batch. Includes all good and bad events.
   * @param badAccumulated
   *   Events that failed for any reason so far.
   * @param tokens
   *   The tokens to be emitted after we have finished processing all events
   */
  private case class BatchAfterTransform(
    toBeInserted: List[EventWithTransform],
    entities: NonAtomicFields.Result,
    origBatchBytes: Long,
    origBatchCount: Long,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(setLatency(env.metrics))
      .through(parseBytes(badProcessor))
      .through(BatchUp.withTimeout(env.batching.maxBytes, env.batching.maxDelay))
      .through(transform(env, badProcessor))
      .through(handleSchemaEvolution(env))
      .through(writeToBigQuery(env, badProcessor))
      .through(sendFailedEvents(env, badProcessor))
      .through(sendMetrics(env))
      .through(emitTokens)
  }

  private def setLatency[F[_]: Sync](metrics: Metrics[F]): Pipe[F, TokenedEvents, TokenedEvents] =
    _.evalTap {
      _.earliestSourceTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            latencyMillis = now.toMillis - t.toEpochMilli
            _ <- metrics.setLatencyMillis(latencyMillis)
          } yield ()
        case None =>
          Applicative[F].unit
      }
    }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Sync](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParseResult] =
    _.evalMap { case TokenedEvents(chunk, token, _) =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { byteBuffer =>
                               Sync[F].delay {
                                 Event.parseBytes(byteBuffer).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(byteBuffer).toString)
                                   BadRow.LoaderParsingError(badProcessor, failure, payload)
                                 }
                               }
                             }
      } yield ParseResult(events, badRows, numBytes, token)
    }

  /** Transform the Event into values compatible with the snowflake ingest sdk */
  private def transform[F[_]: Sync: RegistryLookup](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, Batched, BatchAfterTransform] =
    _.evalMap { case Batched(events, parseFailures, countBytes, entities, tokens) =>
      for {
        now <- Sync[F].realTimeInstant
        loadTstamp = BigQueryCaster.timestampValue(now)
        _ <- Logger[F].debug(s"Processing batch of size ${events.size}")
        nonAtomicFields <- NonAtomicFields.resolveTypes[F](env.resolver, entities)
        (moreBad, rows) <- transformBatch[F](badProcessor, loadTstamp, events, nonAtomicFields)
      } yield BatchAfterTransform(
        rows,
        nonAtomicFields,
        countBytes,
        events.size + parseFailures.size,
        parseFailures.prepend(moreBad),
        tokens
      )
    }

  private def transformBatch[F[_]: Sync](
    badProcessor: BadRowProcessor,
    loadTstamp: java.lang.Long,
    events: ListOfList[Event],
    entities: NonAtomicFields.Result
  ): F[(List[BadRow], List[EventWithTransform])] =
    Foldable[ListOfList]
      .traverseSeparateUnordered(events) { event =>
        Sync[F].delay {
          Transform
            .transformEvent[AnyRef](badProcessor, BigQueryCaster, event, entities)
            .map { namedValues =>
              val map = namedValues
                .map { case Caster.NamedValue(k, v) =>
                  k -> v
                }
                .toMap
                .updated("load_tstamp", loadTstamp)
              event -> map
            }
        }
      }

  private def writeToBigQuery[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.parEvalMap(env.batching.writeBatchConcurrency) { batch =>
      writeUntilSuccessful(env, badProcessor, batch)
        .onError { _ =>
          env.appHealth.setServiceHealth(AppHealth.Service.BigQueryClient, isHealthy = false)
        }
    }

  implicit private class WriteResultOps[F[_]: Async](val attempt: F[Writer.WriteResult]) {

    /**
     * Hard-coded constant. Forgive me.
     *
     * If we recover before reaching this number of retries, then there's really no need to log a
     * known exception. If we exceed this number of retries then it's unexpected, and it would be
     * helpful to have something in the logs instead of hiding the exception.
     */
    private def errorsAllowedWithShortLogging = 4

    def handlingServerSideSchemaMismatches(env: Environment[F]): F[Writer.WriteResult] = {
      def onFailure(wr: Writer.WriteResult, details: RetryDetails): F[Unit] = {
        val extractedDetail = BigQueryRetrying.extractRetryDetails(details)
        val msg             = s"Newly added columns have not yet propagated to the BigQuery Writer. $extractedDetail"
        val log = wr match {
          case Writer.WriteResult.ServerSideSchemaMismatch(e) if details.retriesSoFar > errorsAllowedWithShortLogging =>
            Logger[F].warn(e)(msg)
          case _ =>
            Logger[F].warn(msg)
        }
        log *> env.writer.closed.use_
      }
      attempt
        .retryingOnFailures(
          policy = env.alterTableWaitPolicy,
          wasSuccessful = {
            case Writer.WriteResult.ServerSideSchemaMismatch(_) => false.pure[F]
            case _                                              => true.pure[F]
          },
          onFailure = onFailure
        )
    }

    def handlingWriterWasClosedByEarlierErrors(env: Environment[F]): F[Writer.WriteResult] = {
      def onFailure(wr: Writer.WriteResult, details: RetryDetails): F[Unit] = {
        val msg =
          "BigQuery Writer was already closed by an earlier exception in a different Fiber.  Will reset the Writer and try again immediately"
        val log = wr match {
          case Writer.WriteResult.WriterWasClosedByEarlierError(e) if details.retriesSoFar > errorsAllowedWithShortLogging =>
            Logger[F].warn(e)(msg)
          case _ =>
            Logger[F].warn(msg)
        }
        log *> env.writer.closed.use_
      }
      val retryImmediately = PolicyDecision.DelayAndRetry(Duration.Zero)
      attempt
        .retryingOnFailures(
          policy = RetryPolicy.lift[F](_ => retryImmediately),
          wasSuccessful = {
            case Writer.WriteResult.WriterWasClosedByEarlierError(_) => false.pure[F]
            case _                                                   => true.pure[F]
          },
          onFailure = onFailure
        )
    }
  }

  private def writeUntilSuccessful[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    if (batch.toBeInserted.isEmpty)
      batch.pure[F]
    else
      env.writer.opened
        .use(_.write(batch.toBeInserted.map(_._2)))
        .handlingWriterWasClosedByEarlierErrors(env)
        .handlingServerSideSchemaMismatches(env)
        .flatMap {
          case Writer.WriteResult.SerializationFailures(failures) =>
            val (badRows, tryAgains) = batch.toBeInserted.zipWithIndex.foldLeft((List.empty[BadRow], List.empty[EventWithTransform])) {
              case ((badRows, tryAgains), (eventWithTransform, index)) =>
                failures.get(index) match {
                  case Some(cause) =>
                    val badRow = BadRow.LoaderRuntimeError(badProcessor, cause, BadPayload.LoaderPayload(eventWithTransform._1))
                    (badRow :: badRows, tryAgains)
                  case None =>
                    (badRows, eventWithTransform :: tryAgains)
                }
            }
            writeUntilSuccessful(
              env,
              badProcessor,
              batch.copy(toBeInserted = tryAgains, badAccumulated = batch.badAccumulated.prepend(badRows))
            )
          case _ =>
            Sync[F].pure(batch.copy(toBeInserted = List.empty))
        }

  /**
   * Alters the table to add any columns that were present in the Events but not currently in the
   * table
   */
  private def handleSchemaEvolution[F[_]: Sync](
    env: Environment[F]
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      env.writer.opened
        .use(_.descriptor)
        .flatMap { descriptor =>
          val fields = batch.entities.fields.flatMap { tte =>
            tte.mergedField :: tte.recoveries.map(_._2)
          }
          if (BigQuerySchemaUtils.alterTableRequired(descriptor, fields)) {
            env.tableManager.addColumns(fields) *> env.writer.closed.use_
          } else {
            Sync[F].unit
          }
        }
        .onError { _ =>
          env.appHealth.setServiceHealth(AppHealth.Service.BigQueryClient, isHealthy = false)
        }
    }

  private def sendFailedEvents[F[_]: Sync](
    env: Environment[F],
    badRowProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized =
          batch.badAccumulated.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, badRowProcessor, env.badRowMaxSize))
        env.badSink
          .sinkSimple(serialized)
          .onError { _ =>
            env.appHealth.setServiceHealth(AppHealth.Service.BadSink, isHealthy = false)
          }
      } else Applicative[F].unit
    }

  private def sendMetrics[F[_]: Applicative, A](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      env.metrics.addGood(batch.origBatchCount - batch.badAccumulated.size) *> env.metrics.addBad(batch.badAccumulated.size)
    }

  private def emitTokens[F[_]]: Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] = new BatchUp.Batchable[ParseResult, Batched] {
    def combine(b: Batched, a: ParseResult): Batched =
      Batched(
        events        = b.events.prepend(a.events),
        parseFailures = b.parseFailures.prepend(a.parseFailures),
        countBytes    = b.countBytes + a.countBytes,
        entities      = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_)) |+| b.entities,
        tokens        = b.tokens :+ a.token
      )
    def single(a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(ListOfList.of(List(a.events)), ListOfList.of(List(a.parseFailures)), a.countBytes, entities, Vector(a.token))
    }
    def weightOf(a: ParseResult): Long =
      a.countBytes
  }

}
