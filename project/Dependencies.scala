/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
    // Scala Snowplow
    val analyticsSdk       = "0.4.2"
    val igluClient         = "0.5.0"
    val igluCore           = "0.4.0"
    val processingManifest = "0.1.0"
    val schemaDdl          = "0.9.0"
    // Scala
    val cats               = "1.6.0"
    val catsEffect         = "1.2.0"
    val circe              = "0.11.1"
    val decline            = "0.5.1"
    val fs2                = "1.0.3"
    val json4sJackson      = "3.2.11"
    val scalaz7            = "7.0.9"
    val scio               = "0.6.1"
    // Java
    val beam               = "2.6.0"
    val googleCloud        = "1.51.0"
    val slf4j              = "1.7.25"
    // Scala (test only)
    val specs2             = "4.3.5"
    val scalaCheck         = "1.14.0"
    // Build
    val scalaMacrosVersion = "2.1.0"
    val betterMonadicFor   = "0.2.4"
    val kindProjector      = "0.9.7"
  }

  val bigQuery           = "com.google.cloud"      %  "google-cloud-bigquery"        % V.googleCloud
  val pubsub             = "com.google.cloud"      %  "google-cloud-pubsub"          % V.googleCloud

  // Scala
  val analyticsSdk       = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val igluClient         = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluCoreCirce      = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val processingManifest = "com.snowplowanalytics" %% "snowplow-processing-manifest" % V.processingManifest
  val schemaDdl          = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl

  val cats               = "org.typelevel"         %% "cats-core"                    % V.cats
  val catsEffect         = "org.typelevel"         %% "cats-effect"                  % V.catsEffect
  val circe              = "io.circe"              %% "circe-core"                   % V.circe
  val circeJavaTime      = "io.circe"              %% "circe-java8"                  % V.circe
  val circeParser        = "io.circe"              %% "circe-parser"                 % V.circe
  val circeLiteral       = "io.circe"              %% "circe-literal"                % V.circe        % "test"
  val circeJawn          = "io.circe"              %% "circe-jawn"                   % V.circe
  val decline            = "com.monovore"          %% "decline"                      % V.decline
  val fs2                = "co.fs2"                %% "fs2-core"                     % V.fs2
  val json4sJackson      = "org.json4s"            %% "json4s-jackson"               % V.json4sJackson
  val json4sExt          = "org.json4s"            %% "json4s-ext"                   % V.json4sJackson
  val scalaz7            = "org.scalaz"            %% "scalaz-core"                  % V.scalaz7
  val scioCore           = "com.spotify"           %% "scio-core"                    % V.scio
  val scioRepl           = "com.spotify"           %% "scio-repl"                    % V.scio

  // Java
  val dataflowRunner     = "org.apache.beam"       % "beam-runners-google-cloud-dataflow-java" % V.beam
  val directRunner       = "org.apache.beam"       % "beam-runners-direct-java"                % V.beam
  val slf4j              = "org.slf4j"             % "slf4j-simple"                            % V.slf4j

  // Scala (test only)
  val scalaCheck         = "org.scalacheck"        %% "scalacheck"                   % V.scalaCheck     % "test"
  val specs2             = "org.specs2"            %% "specs2-core"                  % V.specs2         % "test"
  val scioTest           = "com.spotify"           %% "scio-test"                    % V.scio           % "test"
}
