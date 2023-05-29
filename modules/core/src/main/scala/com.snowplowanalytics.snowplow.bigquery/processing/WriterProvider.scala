/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Applicative
import cats.implicits._
import cats.effect.{Async, Poll, Resource, Sync}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.json.JSONArray
import com.google.protobuf.Descriptors
import com.google.cloud.bigquery.storage.v1.{
  BigQueryWriteClient,
  BigQueryWriteSettings,
  Exceptions => BQExceptions,
  GetWriteStreamRequest,
  JsonStreamWriter,
  TableSchema,
  WriteStreamView
}
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings
import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.api.gax.rpc.FixedHeaderProvider

import com.snowplowanalytics.snowplow.bigquery.{Alert, Config, Monitoring}

import scala.jdk.CollectionConverters._

trait WriterProvider[F[_]] {

  def descriptor: F[Descriptors.Descriptor]

  /**
   * Writes rows to BigQuery
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def write(rows: List[Map[String, AnyRef]]): F[WriterProvider.WriteResult]
}

object WriterProvider {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  sealed trait WriteResult

  object WriteResult {

    case object Success extends WriteResult

    /**
     * The result of `write` when it raises an exception which which closed the Writer
     */
    case object WriterIsClosed extends WriteResult

    /**
     * The result of `write` when some events could not be serialized according to the underlying
     * protobuf descriptor
     *
     * This indicates a mis-match in schema between the incoming events and the target table.
     *
     * @param failuresByIndex
     *   A map of failure messages, keyed by the index of the event in the original list of rows. If
     *   an event is not listed in this map, it has **not** been written to BigQuery, but we can
     *   assume that event has no serialization problems.
     */
    case class SerializationFailures(failuresByIndex: Map[Int, String]) extends WriteResult
  }

  def make[F[_]: Async](
    config: Config.BigQuery,
    retries: Config.Retries,
    credentials: Credentials,
    bigqueryHealth: BigQueryHealth[F],
    monitoring: Monitoring[F]
  ): Resource[F, Resource[F, WriterProvider[F]]] =
    for {
      client <- createWriteClient(config, credentials)
    } yield createWriter(config, retries, client, bigqueryHealth, monitoring).map(impl[F](_, bigqueryHealth))

  private def impl[F[_]: Async](
    writer: JsonStreamWriter,
    bigqueryHealth: BigQueryHealth[F]
  ): WriterProvider[F] =
    new WriterProvider[F] {

      def descriptor: F[Descriptors.Descriptor] =
        Sync[F].delay(writer.getDescriptor)

      def write(rows: List[Map[String, AnyRef]]): F[WriteResult] =
        Sync[F]
          .delay(writer.append(new JSONArray(rows.map(_.asJava).asJava)))
          .attemptNarrow[BQExceptions.AppendSerializationError]
          .flatMap[WriteResult] {
            case Right(fut) =>
              FutureInterop
                .fromFuture(fut)
                .as[WriteResult](WriteResult.Success)
                .attemptTap {
                  case Right(_) => bigqueryHealth.setHealthy()
                  case Left(_)  => bigqueryHealth.setUnhealthy()
                }
                .recoverWith {
                  case (e: Throwable) if writer.isClosed =>
                    Logger[F].warn(e)(s"Channel is closed, with an associated exception").as(WriteResult.WriterIsClosed)
                }
            case Left(appendSerializationError) =>
              Applicative[F].pure {
                WriteResult.SerializationFailures {
                  appendSerializationError.getRowIndexToErrorMessage.asScala.map { case (i, cause) =>
                    i.toInt -> cause
                  }.toMap
                }
              }
          }
    }

  private def createWriter[F[_]: Async](
    config: Config.BigQuery,
    retries: Config.Retries,
    client: BigQueryWriteClient,
    bigqueryHealth: BigQueryHealth[F],
    monitoring: Monitoring[F]
  ): Resource[F, JsonStreamWriter] = {
    val streamId = BigQueryUtils.streamIdOf(config)
    def make(poll: Poll[F]) = poll {
      BigQueryRetrying.withRetries(bigqueryHealth, retries, monitoring, Alert.FailedToOpenBigQueryWriter(_)) {
        for {
          _ <- Logger[F].info(show"Opening BigQuery stream writer: $streamId")
          // Set the table schema explicitly, to prevent the JsonStreamWriter from fetching it automatically when we're not in control.
          schema <- getTableSchema[F](client, config)
          w <- Sync[F].blocking(JsonStreamWriter.newBuilder(streamId, schema, client).build)
        } yield w
      }
    }
    Resource.makeFull(make) { w =>
      Logger[F].info("Closing BigQuery stream writer...") *>
        Sync[F].blocking(w.close())
    }
  }

  private def createWriteClient[F[_]: Sync](
    config: Config.BigQuery,
    credentials: Credentials
  ): Resource[F, BigQueryWriteClient] = {
    val make = Sync[F].delay {
      val settingsBuilder = BigQueryWriteSettings.newBuilder
        .setHeaderProvider(FixedHeaderProvider.create("user-agent", s"${config.gcpUserAgent.productName}/bigquery-loader (GPN:Snowplow;)"))
      settingsBuilder.getStubSettingsBuilder.setCredentialsProvider(credentialsProvider(credentials))
      BigQueryWriteClient.create(settingsBuilder.build)
    }
    Resource.make(make)(c => Sync[F].blocking(c.close()))
  }

  private def credentialsProvider(credentials: Credentials): CredentialsProvider =
    new CredentialsProvider {
      def getCredentials(): Credentials = credentials match {
        case sac: ServiceAccountCredentials =>
          if (sac.createScopedRequired)
            // TODO: Is this needed?
            sac.createScoped(BigQueryWriteStubSettings.getDefaultServiceScopes) match {
              case sac: ServiceAccountCredentials =>
                sac.createWithUseJwtAccessWithScope(true)
              case other => other
            }
          else
            sac.createWithUseJwtAccessWithScope(true)
        case other => other
      }
    }

  private def getTableSchema[F[_]: Sync](client: BigQueryWriteClient, config: Config.BigQuery): F[TableSchema] = {
    val streamId = BigQueryUtils.streamIdOf(config)
    val request  = GetWriteStreamRequest.newBuilder.setName(streamId).setView(WriteStreamView.FULL).build

    Sync[F]
      .blocking(client.getWriteStream(request))
      .map(_.getTableSchema)
  }

}
