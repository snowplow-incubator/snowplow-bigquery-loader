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

lazy val loader = project
  .in(file("modules/loader"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)
  .settings(BuildSettings.loaderBuildSettings)
  .dependsOn(common % "compile->compile;test->test")

lazy val loaderDistroless = project
  .in(file("modules/distroless/loader"))
  .enablePlugins(BuildInfoPlugin, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (loader / sourceDirectory).value)
  .settings(BuildSettings.loaderDistrolessBuildSettings)
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
