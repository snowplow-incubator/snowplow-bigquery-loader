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
    // Scala
    val scio               = "0.5.4"
    val decline            = "0.4.0"
    val analyticsSdk       = "0.3.0"
    val cats               = "1.1.0"
    val processingManifest = "0.1.0-M4"
    val igluClient         = "0.5.0"
    val igluCore           = "0.2.0"
    val circe              = "0.9.3"
    val scalaz7            = "7.0.9"
    val json4sJackson      = "3.2.11"
    // Java
    val beam               = "2.4.0"
    val scalaMacrosVersion = "2.1.0"
    val slf4j              = "1.7.25"
    // Scala (test only)
    val specs2             = "4.0.4"
    val scalaCheck         = "1.13.4"
  }

  // Scala
  val scioCore           = "com.spotify"           %% "scio-core"                    % V.scio
  val scioRepl           = "com.spotify"           %% "scio-repl"                    % V.scio
  val analyticsSdk       = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val decline            = "com.monovore"          %% "decline"                      % V.decline
  val cats               = "org.typelevel"         %% "cats-core"                    % V.cats
  val processingManifest = "com.snowplowanalytics" %% "snowplow-processing-manifest" % V.processingManifest
  val igluClient         = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluCoreCirce      = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val circe              = "io.circe"              %% "circe-core"                   % V.circe
  val circeJavaTime      = "io.circe"              %% "circe-java8"                  % V.circe
  val scalaz7            = "org.scalaz"            %% "scalaz-core"                  % V.scalaz7
  val json4sJackson      = "org.json4s"            %% "json4s-jackson"               % V.json4sJackson

  // Java
  val directRunner       = "org.apache.beam"       % "beam-runners-direct-java"                % V.beam
  val dataflowRunner     = "org.apache.beam"       % "beam-runners-google-cloud-dataflow-java" % V.beam
  val slf4j              = "org.slf4j"             % "slf4j-simple"                            % V.slf4j

  // Scala (test only)
  val specs2             = "org.specs2"            %% "specs2-core"                  % V.specs2         % "test"
  val scalaCheck         = "org.scalacheck"        %% "scalacheck"                   % V.scalaCheck     % "test"
  val scioTest           = "com.spotify"           %% "scio-test"                    % V.scio           % "test"
}
