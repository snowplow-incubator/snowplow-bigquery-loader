/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    kafka,
    kafkaDistroless,
    pubsub,
    pubsubDistroless,
    kinesis,
    kinesisDistroless
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.coreSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)
  .enablePlugins(IgluSchemaPlugin)

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kafkaDistroless: Project = project
  .in(file("modules/distroless/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val pubsub: Project = project
  .in(file("modules/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val pubsubDistroless: Project = project
  .in(file("modules/distroless/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val kinesis: Project = project
  .in(file("modules/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kinesisDistroless: Project = project
  .in(file("modules/distroless/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

ThisBuild / fork := true
