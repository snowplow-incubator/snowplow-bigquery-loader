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
import Keys._
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.ExecCmd
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._

object BuildSettings {
  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    version := "0.5.1",
    scalaVersion := "2.13.2",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description, BuildInfoKey.action("userAgent") {
      s"${name.value}/${version.value}"
    })
  )

  lazy val commonProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-common",
    description := "Snowplow BigQuery Loader Common Utils"
  )

  lazy val loaderProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-loader",
    description := "Snowplow BigQuery Loader Dataflow Job",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.loader.generated"
  )

  lazy val mutatorProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Table Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bigquery.mutator.Main"),
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.mutator.generated"
  )

  lazy val repeaterProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-repeater",
    description := "Snowplow BigQuery Java app for replaying events from failed inserts subscription",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.repeater.generated"
  )

  lazy val forwarderProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-forwarder",
    description := "This component is deprecated from version 0.5.0 on. Use BigQuery Repeater instead.",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.forwarder.generated"
  )

  // TODO: remove?
  lazy val buildInfoSettings = Seq(
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.generated"
  )

  // Make package (build) metadata available within source code.
  lazy val scalifiedSettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(
        file,
        """package %s
          |object ProjectMetadata {
          |  val organization = "%s"
          |  val name = "%s"
          |  val version = "%s"          
          |  val scalaVersion = "%s"
          |  val description = "%s"
          |}
          |"""
          .stripMargin
          .format(
            buildInfoPackage.value,
            organization.value,
            name.value,
            version.value,
            scalaVersion.value,
            description.value
          )
      )
      Seq(file)
    }.taskValue
  )

  lazy val compilerSettings = Seq(
    javacOptions := Seq(
      "-source",
      "1.8",
      "-target",
      "1.8",
      "-Xlint"
    )
  )

  lazy val resolverSettings = Seq(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots".at("https://oss.sonatype.org/content/repositories/snapshots/"),
      "Snowplow Bintray".at("https://snowplow.bintray.com/snowplow-maven/")
    )
  )

  lazy val macroSettings = Seq(
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

  lazy val dockerSettings = Seq(
    // Use single entrypoint script for all apps
    Universal / sourceDirectory := new java.io.File((baseDirectory in LocalRootProject).value, "docker"),
    dockerRepository := Some("snowplow-docker-registry.bintray.io"),
    dockerUsername := Some("snowplow"),
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    Docker / daemonUser := "root", // Will be gosu'ed by docker-entrypoint.sh
    dockerEnvVars := Map("SNOWPLOW_BIGQUERY_APP" -> name.value),
    dockerCommands += ExecCmd("RUN", "cp", "/opt/docker/bin/docker-entrypoint.sh", "/usr/local/bin/"),
    dockerEntrypoint := Seq("docker-entrypoint.sh"),
    dockerCmd := Seq("--help")
  )

  lazy val buildSettings = Seq(
    Global / cancelable := true,
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Dependencies.V.betterMonadicFor),
    addCompilerPlugin(("org.typelevel" %% "kind-projector" % Dependencies.V.kindProjector).cross(CrossVersion.full))
  ) ++ compilerSettings ++ resolverSettings ++ dockerSettings

  lazy val commonBuildSettings    = (commonProjectSettings ++ buildSettings).diff(dockerSettings)
  lazy val loaderBuildSettings    = loaderProjectSettings ++ buildSettings ++ scalifiedSettings ++ macroSettings
  lazy val mutatorBuildSettings   = mutatorProjectSettings ++ buildSettings
  lazy val repeaterBuildSettings  = repeaterProjectSettings ++ buildSettings ++ scalifiedSettings ++ macroSettings
  lazy val forwarderBuildSettings = forwarderProjectSettings ++ buildSettings ++ macroSettings
}
