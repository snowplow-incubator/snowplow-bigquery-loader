/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery.common.config

import cats.implicits.toFunctorOps
import cats.syntax.either._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import scala.concurrent.duration.FiniteDuration

object model {
  sealed trait Config extends Product with Serializable {
    val input: Input
  }
  object Config {
    final case class Loader(
      input: Input.PubSub,
      output: LoaderOutputs,
      loadMode: LoadMode,
      sinkSettings: SinkSettings
    ) extends Config
    final case class Mutator(input: Input.PubSub, output: MutatorOutput) extends Config
    final case class Repeater(input: Input.PubSub, output: RepeaterOutputs) extends Config

    final case class LoaderOutputs(
      good: Output.BigQuery,
      bad: Output.PubSub,
      types: Output.PubSub,
      failedInserts: Output.PubSub
    )
    final case class RepeaterOutputs(good: Output.BigQuery, deadLetters: Output.Gcs)
    final case class MutatorOutput(good: Output.BigQuery)

    implicit val configLoaderEncoder: Encoder[Loader] = deriveEncoder[Loader]
    implicit val configLoaderDecoder: Decoder[Loader] = deriveDecoder[Loader]

    implicit val configMutatorEncoder: Encoder[Mutator] = deriveEncoder[Mutator]
    implicit val configMutatorDecoder: Decoder[Mutator] = deriveDecoder[Mutator]

    implicit val configRepeaterEncoder: Encoder[Repeater] = deriveEncoder[Repeater]
    implicit val configRepeaterDecoder: Decoder[Repeater] = deriveDecoder[Repeater]

    implicit val loaderOutputsEncoder: Encoder[LoaderOutputs] = deriveEncoder[LoaderOutputs]
    implicit val loaderOutputsDecoder: Decoder[LoaderOutputs] = deriveDecoder[LoaderOutputs]

    implicit val repeaterOutputsEncoder: Encoder[RepeaterOutputs] = deriveEncoder[RepeaterOutputs]
    implicit val repeaterOutputsDecoder: Decoder[RepeaterOutputs] = deriveDecoder[RepeaterOutputs]

    implicit val mutatorOutputEncoder: Encoder[MutatorOutput] = deriveEncoder[MutatorOutput]
    implicit val mutatorOutputDecoder: Decoder[MutatorOutput] = deriveDecoder[MutatorOutput]
  }

  sealed trait Input extends Product with Serializable
  object Input {
    final case class PubSub private (subscription: String) extends Input

    implicit val inputPubSubEncoder: Encoder[Input.PubSub] = deriveEncoder[Input.PubSub]
    implicit val inputPubSubDecoder: Decoder[Input.PubSub] = deriveDecoder[Input.PubSub]
  }

  sealed trait Output extends Product with Serializable
  object Output {
    final case class BigQuery private (datasetId: String, tableId: String) extends Output
    final case class PubSub private (topic: String) extends Output
    final case class Gcs private (bucket: String) extends Output

    implicit val outputBigQueryEncoder: Encoder[Output.BigQuery] = deriveEncoder[Output.BigQuery]
    implicit val outputBigQueryDecoder: Decoder[Output.BigQuery] = deriveDecoder[Output.BigQuery]

    implicit val outputPubSubEncoder: Encoder[Output.PubSub] = deriveEncoder[Output.PubSub]
    implicit val outputPubSubDecoder: Decoder[Output.PubSub] = deriveDecoder[Output.PubSub]

    implicit val outputGcsEncoder: Encoder[Output.Gcs] = deriveEncoder[Output.Gcs]
    implicit val outputGcsDecoder: Decoder[Output.Gcs] = deriveDecoder[Output.Gcs]
  }

  sealed trait LoadMode extends Product with Serializable
  object LoadMode {
    final case class StreamingInserts private (retry: Boolean) extends LoadMode
    final case class FileLoads private (frequency: Int) extends LoadMode

    implicit val loadModeEncoder: Encoder[LoadMode] = Encoder.instance {
      case s: StreamingInserts => deriveEncoder[StreamingInserts].apply(s)
      case f: FileLoads        => deriveEncoder[FileLoads].apply(f)
    }

    implicit val loadModeDecoder: Decoder[LoadMode] = List[Decoder[LoadMode]](
      deriveDecoder[StreamingInserts].widen,
      deriveDecoder[FileLoads].widen
    ).reduceLeft(_.or(_))
  }

  final case class SinkSettings(
    good: SinkSettings.Good,
    bad: SinkSettings.Bad,
    types: SinkSettings.Types,
    failedInserts: SinkSettings.FailedInserts
  )
  object SinkSettings {
    final case class Good(
      bqWriteRequestThreshold: Int,
      bqWriteRequestTimeout: FiniteDuration,
      bqWriteRequestSizeLimit: Int,
      bqWriteRequestOverflowQueueMaxSize: Int,
      sinkConcurrency: Int
    )

    final case class Bad(producerBatchSize: Long, producerDelayThreshold: FiniteDuration, sinkConcurrency: Int)

    final case class Types(
      batchThreshold: Int,
      batchTimeout: FiniteDuration,
      producerBatchSize: Long,
      producerDelayThreshold: FiniteDuration,
      sinkConcurrency: Int
    )

    final case class FailedInserts(producerBatchSize: Long, producerDelayThreshold: FiniteDuration)

    implicit val sinkSettingsEncoder: Encoder[SinkSettings] = deriveEncoder[SinkSettings]
    implicit val sinkSettingsDecoder: Decoder[SinkSettings] = deriveDecoder[SinkSettings]

    implicit val sinkSettingsGoodEncoder: Encoder[SinkSettings.Good] = deriveEncoder[SinkSettings.Good]
    implicit val sinkSettingsGoodDecoder: Decoder[SinkSettings.Good] = deriveDecoder[SinkSettings.Good]

    implicit val sinkSettingsBadEncoder: Encoder[SinkSettings.Bad] = deriveEncoder[SinkSettings.Bad]
    implicit val sinkSettingsBadDecoder: Decoder[SinkSettings.Bad] = deriveDecoder[SinkSettings.Bad]

    implicit val sinkSettingsTypesEncoder: Encoder[SinkSettings.Types] = deriveEncoder[SinkSettings.Types]
    implicit val sinkSettingsTypesDecoder: Decoder[SinkSettings.Types] = deriveDecoder[SinkSettings.Types]

    implicit val sinkSettingsFailedInsertsEncoder: Encoder[SinkSettings.FailedInserts] =
      deriveEncoder[SinkSettings.FailedInserts]
    implicit val sinkSettingsFailedInsertsDecoder: Decoder[SinkSettings.FailedInserts] =
      deriveDecoder[SinkSettings.FailedInserts]
  }

  final case class Monitoring(statsd: Option[Monitoring.Statsd], dropwizard: Option[Monitoring.Dropwizard])
  object Monitoring {
    final case class Statsd(
      hostname: String,
      port: Int,
      tags: Map[String, String],
      period: FiniteDuration,
      prefix: Option[String]
    )
    final case class Dropwizard(period: FiniteDuration)

    implicit val monitoringEncoder: Encoder[Monitoring] = deriveEncoder[Monitoring]
    implicit val monitoringDecoder: Decoder[Monitoring] = deriveDecoder[Monitoring]

    implicit val monitoringStatsdEncoder: Encoder[Statsd] = deriveEncoder[Statsd]
    implicit val monitoringStatsdDecoder: Decoder[Statsd] = deriveDecoder[Statsd]

    implicit val monitoringDropwizardEncoder: Encoder[Dropwizard] = deriveEncoder[Dropwizard]
    implicit val monitoringDropwizardDecoder: Decoder[Dropwizard] = deriveDecoder[Dropwizard]
  }

  implicit val finiteDurationEncoder: Encoder[FiniteDuration] =
    implicitly[Encoder[String]].contramap(_.toString)
  implicit val finiteDurationDecoder: Decoder[FiniteDuration] =
    implicitly[Decoder[String]].emap { s =>
      val strSplit       = s.split(" ")
      val (length, unit) = (strSplit(0).toLong, strSplit(1))
      Either.catchOnly[NumberFormatException](FiniteDuration(length, unit)).leftMap(_.toString)
    }
}
