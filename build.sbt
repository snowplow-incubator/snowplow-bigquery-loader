/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
  .aggregate(loaderDistroless, streamloaderDistroless, mutatorDistroless, repeaterDistroless)
  .settings(assembly / aggregate := false)
// format: on

lazy val common = project
  .in(file("modules/common"))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.commonBuildSettings)
<<<<<<< HEAD
=======
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Beam.bigQuery,
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
      Dependencies.googleOauth,
      Dependencies.gson,
      Dependencies.sentry
    )
  )
>>>>>>> parent of ef0228a (Remove explicit version overrides of gson and fast-json (close #280))

lazy val loader = project
  .in(file("modules/loader"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)
  .settings(BuildSettings.loaderBuildSettings)
<<<<<<< HEAD
  .dependsOn(common % "compile->compile;test->test")

lazy val loaderDistroless = project
  .in(file("modules/distroless/loader"))
  .enablePlugins(BuildInfoPlugin, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (loader / sourceDirectory).value)
  .settings(BuildSettings.loaderDistrolessBuildSettings)
=======
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
>>>>>>> parent of ef0228a (Remove explicit version overrides of gson and fast-json (close #280))
  .dependsOn(common % "compile->compile;test->test")

lazy val streamloader = project
  .in(file("modules/streamloader"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)
  .settings(BuildSettings.streamloaderBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val streamloaderDistroless = project
  .in(file("modules/distroless/streamloader"))
  .enablePlugins(BuildInfoPlugin, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamloader / sourceDirectory).value)
  .settings(BuildSettings.streamloaderDistrolessBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val mutator = project
  .in(file("modules/mutator"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)
  .settings(BuildSettings.mutatorBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val mutatorDistroless = project
  .in(file("modules/distroless/mutator"))
  .enablePlugins(BuildInfoPlugin, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (mutator / sourceDirectory).value)
  .settings(BuildSettings.mutatorDistrolessBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val repeater = project
  .in(file("modules/repeater"))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(BuildSettings.repeaterBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val repeaterDistroless = project
  .in(file("modules/distroless/repeater"))
  .enablePlugins(BuildInfoPlugin, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (repeater / sourceDirectory).value)
  .settings(BuildSettings.repeaterDistrolessBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

// format: off
lazy val benchmark = project
  .in(file("modules/benchmark"))
  .enablePlugins(JmhPlugin)
  .dependsOn(loader % "test->test")
// format: on
