/*
 * Copyright (c) 2018-2020 Snowplow Analytics Ltd. All rights reserved.
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

  val ResolutionRepos = Seq(
    // Speed-up build
    "snowplow" at "https://snowplow.bintray.com/snowplow-maven"
  )

  object V {
    // Scala Snowplow
    val analyticsSdk       = "0.4.2"
    val igluClient         = "0.6.2"
    val igluCore           = "0.5.0"
    val processingManifest = "0.1.0"
    val schemaDdl          = "0.9.0"
    val badrows            = "0.1.0"
    // Scala
    val cats               = "1.6.1"
    val catsEffect         = "1.4.0"
    val circe              = "0.11.1"
    val decline            = "0.6.2"
    val fs2                = "1.0.5"
    val pubsubFs2          = "0.12.0"
    val httpClient         = "0.20.0"
    val logging            = "0.3.0"
    val scio               = "0.8.1"
    // Java
    val beam               = "2.18.0"
    val googleCloud        = "1.51.0"
    val slf4j              = "1.7.25"
    // Scala (test only)
    val specs2             = "4.3.6"
    val scalaCheck         = "1.14.0"
    // Build
    val scalaMacrosVersion = "2.1.0"
    val betterMonadicFor   = "0.2.4"
    val kindProjector      = "0.9.9"
  }

  val bigQuery           = "com.google.cloud"      %  "google-cloud-bigquery"        % V.googleCloud
  val pubsub             = "com.google.cloud"      %  "google-cloud-pubsub"          % V.googleCloud
  val gcs                = "com.google.cloud"      %  "google-cloud-storage"         % V.googleCloud

  // Scala
  val analyticsSdk       = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val igluClient         = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluCoreCirce      = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val processingManifest = "com.snowplowanalytics" %% "snowplow-processing-manifest" % V.processingManifest
  val schemaDdl          = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val badrows            = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badrows

  val cats               = "org.typelevel"         %% "cats-core"                    % V.cats
  val catsEffect         = "org.typelevel"         %% "cats-effect"                  % V.catsEffect
  val circe              = "io.circe"              %% "circe-core"                   % V.circe
  val circeJavaTime      = "io.circe"              %% "circe-java8"                  % V.circe
  val circeParser        = "io.circe"              %% "circe-parser"                 % V.circe
  val circeLiteral       = "io.circe"              %% "circe-literal"                % V.circe
  val circeJawn          = "io.circe"              %% "circe-jawn"                   % V.circe
  val decline            = "com.monovore"          %% "decline"                      % V.decline
  val fs2                = "co.fs2"                %% "fs2-core"                     % V.fs2
  val scioCore           = "com.spotify"           %% "scio-core"                    % V.scio
  val scioRepl           = "com.spotify"           %% "scio-repl"                    % V.scio
  val pubsubFs2          = "com.permutive"         %% "fs2-google-pubsub-http"       % V.pubsubFs2
  val pubsubFs2Grpc      = "com.permutive"         %% "fs2-google-pubsub-grpc"       % V.pubsubFs2
  val httpClient         = "org.http4s"            %% "http4s-async-http-client"     % V.httpClient
  val logging            = "io.chrisdavenport"     %% "log4cats-slf4j"               % V.logging
  val scioBigQuery       = "com.spotify"           %% "scio-bigquery"                % V.scio

  // Java
  val dataflowRunner     = "org.apache.beam"       % "beam-runners-google-cloud-dataflow-java" % V.beam
  val directRunner       = "org.apache.beam"       % "beam-runners-direct-java"                % V.beam
  val slf4j              = "org.slf4j"             % "slf4j-simple"                            % V.slf4j

  // Scala (test only)
  val scalaCheck         = "org.scalacheck"        %% "scalacheck"                   % V.scalaCheck     % "test"
  val specs2             = "org.specs2"            %% "specs2-core"                  % V.specs2         % "test"
  val specs2ScalaCheck   = "org.specs2"            %% "specs2-scalacheck"            % V.specs2         % "test"
  val scioTest           = "com.spotify"           %% "scio-test"                    % V.scio           % "test"
}
