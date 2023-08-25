/*
 * Copyright (c) 2018-2023 Snowplow Analytics Ltd. All rights reserved.
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
    val googleCloudBigQuery = "2.31.1" // compatible with google-cloud-bom:0.175.0
    val googleCloudPubSub   = "1.119.0" // compatible with google-cloud-bom:0.175.0
    val googleCloudStorage  = "2.7.2" // compatible with google-cloud-bom:0.175.0
    val slf4j   = "1.7.32"
    val sentry  = "1.7.30"

    // Override provided versions to fix security vulnerabilities
    val nettyCodec   = "4.1.86.Final"
    val googleOauth  = "1.33.3"
    val jackson      = "2.14.1"
    val protobuf     = "3.21.12"

    // Scala third-party
    val cats        = "2.6.1"
    val catsEffect  = "3.3.12"
    val catsRetry   = "3.1.0"
    val circe       = "0.14.1"
    val decline     = "1.4.0"
    val fs2         = "3.2.8"
    val httpClient  = "0.23.18"
    val logging     = "2.3.1"
    val pubsubFs2   = "0.20.0"
    val circeConfig = "0.8.0"

    // Scala Snowplow
    val analyticsSdk = "2.1.0"
    val badrows      = "2.2.0"
    val igluClient   = "2.2.0"
    val igluCore     = "1.0.1"
    val schemaDdl    = "0.14.4"

    // Scala (test only)
    val specs2 = "4.13.2"

    // Build
    val betterMonadicFor   = "0.3.1"
    val kindProjector      = "0.13.2"
    val scalaMacrosVersion = "2.1.0"
  }

  // GCP
  val bigQuery = "com.google.cloud" % "google-cloud-bigquery" % V.googleCloudBigQuery
  val pubsub   = "com.google.cloud" % "google-cloud-pubsub"   % V.googleCloudPubSub
  val gcs      = "com.google.cloud" % "google-cloud-storage"  % V.googleCloudStorage

  // Java
  val slf4j           = "org.slf4j"                  % "slf4j-simple"                            % V.slf4j
  val nettyCodec      = "io.netty"                   % "netty-codec"                             % V.nettyCodec
  val nettyCodecHttp  = "io.netty"                   % "netty-codec-http"                        % V.nettyCodec
  val nettyCodecHttp2 = "io.netty"                   % "netty-codec-http2"                       % V.nettyCodec
  val googleOauth     = "com.google.oauth-client"    % "google-oauth-client"                     % V.googleOauth
  val sentry          = "io.sentry"                  % "sentry"                                  % V.sentry
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind"                        % V.jackson
  val protobuf        = "com.google.protobuf"        % "protobuf-java"                           % V.protobuf

  // Scala third-party
  val cats          = "org.typelevel"    %% "cats-core"                  % V.cats
  val catsEffect    = "org.typelevel"    %% "cats-effect"                % V.catsEffect
  val catsRetry     = "com.github.cb372" %% "cats-retry"                 % V.catsRetry
  val circe         = "io.circe"         %% "circe-core"                 % V.circe
  val circeJawn     = "io.circe"         %% "circe-jawn"                 % V.circe
  val circeLiteral  = "io.circe"         %% "circe-literal"              % V.circe
  val circeParser   = "io.circe"         %% "circe-parser"               % V.circe
  val decline       = "com.monovore"     %% "decline"                    % V.decline
  val fs2           = "co.fs2"           %% "fs2-core"                   % V.fs2
  val httpClient    = "org.http4s"       %% "http4s-ember-client"        % V.httpClient
  val logging       = "org.typelevel"    %% "log4cats-slf4j"             % V.logging
  val pubsubFs2Grpc = "com.permutive"    %% "fs2-google-pubsub-grpc"     % V.pubsubFs2
  val circeConfig   = "io.circe"         %% "circe-config"               % V.circeConfig

  // Scala Snowplow
  val analyticsSdk  = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val badrows       = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badrows
  val igluClient    = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluHttp4s    = "com.snowplowanalytics" %% "iglu-scala-client-http4s"     % V.igluClient
  val igluCoreCirce = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val schemaDdl     = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl

  // Scala (test only)
  val specs2   = "org.specs2"  %% "specs2-core" % V.specs2 % "test"


  val commonDependencies = Seq(
      bigQuery,
      cats,
      catsEffect,
      catsRetry,
      circe,
      circeConfig,
      circeJawn,
      circeLiteral,
      circeParser,
      decline,
      fs2,
      googleOauth,
      logging,
      slf4j,
      analyticsSdk,
      badrows,
      igluClient,
      igluCoreCirce,
      jacksonDatabind,
      protobuf,
      schemaDdl,
      specs2,
      nettyCodec,
      nettyCodecHttp,
      nettyCodecHttp2,
      sentry
    )

  val streamloaderDependencies = Seq(
      bigQuery,
      httpClient,
      igluHttp4s,
      pubsubFs2Grpc,
    )

  val mutatorDependencies = Seq(
      bigQuery,
      pubsub
    )

  val repeaterDependencies = Seq(
      gcs,
      httpClient,
      pubsub,
      bigQuery,
      pubsubFs2Grpc
    )
}
