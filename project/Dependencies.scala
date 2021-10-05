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
import sbt._

object Dependencies {

  object V {
    // Java
    val beam                = "2.24.0"
    val googleCloudBigQuery = "1.102.0" // no upgrade before bumping pubsubFs2
    val googleCloudPubSub   = "1.102.0" // no upgrade before bumping pubsubFs2
    val googleCloudStorage  = "1.102.0" // no upgrade before bumping pubsubFs2
    val metrics             = "4.1.25"
    val slf4j               = "1.7.32"

    // TODO: remove when libraries bringing in older versions are bumped
    val nettyCodec     = "4.1.68.Final" // scio
    val nettyCodecHttp = "4.1.44.Final" // scio
    val fastjson       = "1.2.69"       // scio
    val googleOauth    = "1.31.0"       // scio

    // Scala third-party
    val cats       = "2.6.1"
    val catsEffect = "2.5.4"
    val circe      = "0.14.1"
    val decline    = "1.0.0"
    val fs2        = "2.5.0"
    val httpClient = "0.21.15"
    val logging    = "1.1.1"
    // =========================================================================
    // After 1.102.0 the Google Cloud Java SDK versions diverge.
    // It becomes harder to know what versions are compatible with each other.
    // Bumping pubsubFs2 to 0.16.0 calls for googleCloudPubSub 1.107.0,
    // but it's unclear to what versions the other two SDKs need to be bumped.
    // Runtime errors occur when the versions are incompatible.
    // =========================================================================
    val pubsubFs2   = "0.15.0"
    val scio        = "0.9.6"
    val circeConfig = "0.8.0"

    // Scala Snowplow
    val analyticsSdk = "2.1.0"
    val badrows      = "2.1.0"
    val igluClient   = "1.0.2"
    val igluCore     = "1.0.0"
    val schemaDdl    = "0.14.2"

    // Scala (test only)
    val scalaCheck = "1.14.3"
    val specs2     = "4.10.0"

    // Build
    val betterMonadicFor   = "0.3.1"
    val kindProjector      = "0.11.0"
    val scalaMacrosVersion = "2.1.0"
  }

  // GCP
  val bigQuery = "com.google.cloud" % "google-cloud-bigquery" % V.googleCloudBigQuery
  val pubsub   = "com.google.cloud" % "google-cloud-pubsub"   % V.googleCloudPubSub
  val gcs      = "com.google.cloud" % "google-cloud-storage"  % V.googleCloudStorage

  // Java
  val dataflowRunner = "org.apache.beam"         % "beam-runners-google-cloud-dataflow-java" % V.beam
  val directRunner   = "org.apache.beam"         % "beam-runners-direct-java"                % V.beam
  val metrics        = "io.dropwizard.metrics"   % "metrics-core"                            % V.metrics
  val slf4j          = "org.slf4j"               % "slf4j-simple"                            % V.slf4j
  val nettyCodec     = "io.netty"                % "netty-codec"                             % V.nettyCodec
  val nettyCodecHttp = "io.netty"                % "netty-codec-http"                        % V.nettyCodecHttp
  val fastjson       = "com.alibaba"             % "fastjson"                                % V.fastjson
  val googleOauth    = "com.google.oauth-client" % "google-oauth-client"                     % V.googleOauth

  // Scala third-party
  val cats          = "org.typelevel"     %% "cats-core"                % V.cats
  val catsEffect    = "org.typelevel"     %% "cats-effect"              % V.catsEffect
  val circe         = "io.circe"          %% "circe-core"               % V.circe
  val circeJawn     = "io.circe"          %% "circe-jawn"               % V.circe
  val circeLiteral  = "io.circe"          %% "circe-literal"            % V.circe
  val circeParser   = "io.circe"          %% "circe-parser"             % V.circe
  val decline       = "com.monovore"      %% "decline"                  % V.decline
  val fs2           = "co.fs2"            %% "fs2-core"                 % V.fs2
  val httpClient    = "org.http4s"        %% "http4s-async-http-client" % V.httpClient
  val logging       = "io.chrisdavenport" %% "log4cats-slf4j"           % V.logging
  val pubsubFs2     = "com.permutive"     %% "fs2-google-pubsub-http"   % V.pubsubFs2
  val pubsubFs2Grpc = "com.permutive"     %% "fs2-google-pubsub-grpc"   % V.pubsubFs2
  val scioBigQuery  = "com.spotify"       %% "scio-bigquery"            % V.scio
  val scioCore      = "com.spotify"       %% "scio-core"                % V.scio
  val scioRepl      = "com.spotify"       %% "scio-repl"                % V.scio
  val circeConfig   = "io.circe"          %% "circe-config"             % V.circeConfig

  // Scala Snowplow
  val analyticsSdk  = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val badrows       = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badrows
  val igluClient    = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluCoreCirce = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val schemaDdl     = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl

  // Scala (test only)
  val scalaCheck       = "org.scalacheck" %% "scalacheck"        % V.scalaCheck % "test"
  val specs2           = "org.specs2"     %% "specs2-core"       % V.specs2     % "test"
  val specs2ScalaCheck = "org.specs2"     %% "specs2-scalacheck" % V.specs2     % "test"
  val scioTest         = "com.spotify"    %% "scio-test"         % V.scio       % "test"
}
