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

// format: off
lazy val root = project
  .in(file("."))
  .settings(name := "bigquery-loader")
  .aggregate(common, loader, streamloader, mutator, repeater)
  .settings(assembly / aggregate := false)
// format: on

lazy val common = project
  .in(file("modules/common"))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.commonBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.bigQuery,
      Dependencies.cats,
      Dependencies.catsEffect,
      Dependencies.circe,
      Dependencies.circeConfig,
      Dependencies.circeJawn,
      Dependencies.circeLiteral,
      Dependencies.circeParser,
      Dependencies.decline,
      Dependencies.fs2,
      Dependencies.logging,
      Dependencies.slf4j,
      Dependencies.analyticsSdk,
      Dependencies.badrows,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.schemaDdl,
      Dependencies.specs2,
      Dependencies.nettyCodec,
      Dependencies.nettyCodecHttp,
      Dependencies.nettyCodecHttp2,
      Dependencies.googleOauth
    )
  )

lazy val loader = project
  .in(file("modules/loader"))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.loaderBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.dataflowRunner,
      Dependencies.directRunner,
      Dependencies.metrics,
      Dependencies.scioCore,
      Dependencies.scioGoogle,
      Dependencies.scioTest,
      Dependencies.fastjson
    ),
    resolvers += "Confluent Repository".at("https://packages.confluent.io/maven/")
  )
  .dependsOn(common % "compile->compile;test->test")

lazy val streamloader = project
  .in(file("modules/streamloader"))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.streamloaderBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.pubsubFs2Grpc
    )
  )
  .dependsOn(common % "compile->compile;test->test")

lazy val mutator = project
  .in(file("modules/mutator"))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.mutatorBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.pubsub
    )
  )
  .dependsOn(common % "compile->compile;test->test")

lazy val repeater = project
  .in(file("modules/repeater"))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.repeaterBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.gcs,
      Dependencies.httpClient,
      Dependencies.pubsub,
      Dependencies.pubsubFs2Grpc
    )
  )
  .dependsOn(common % "compile->compile;test->test")

// format: off
lazy val benchmark = project
  .in(file("modules/benchmark"))
  .enablePlugins(JmhPlugin)
  .dependsOn(loader % "test->test")
// format: on
