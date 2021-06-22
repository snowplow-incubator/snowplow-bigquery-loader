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
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

object model {
  sealed trait Config extends Product with Serializable {
    val input: Input
  }
  object Config {
    final case class Loader(input: Input.PubSub, output: LoaderOutputs, loadMode: LoadMode) extends Config
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
}
