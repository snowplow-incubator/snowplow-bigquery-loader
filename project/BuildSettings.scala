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
import com.typesafe.sbt.SbtNativePackager.autoImport._

import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._

import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._
import sbtdynver.DynVerPlugin.autoImport._

object BuildSettings {
  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.9",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description, BuildInfoKey.action("userAgent") {
      s"${name.value}/${version.value}"
    }),
    // Do not warn about Docker keys not being used by any other settings/tasks
    Global    / lintUnusedKeysOnLoad := false,
    ThisBuild / dynverVTagPrefix := false,
    ThisBuild / dynverSeparator := "-"
  )

  lazy val commonProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-common",
    description := "Snowplow BigQuery Loader Common Utils",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.common.generated",
    libraryDependencies ++= Dependencies.commonDependencies
  )

  lazy val streamloaderProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-streamloader",
    description := "Snowplow BigQuery Loader Standalone App",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.streamloader.generated",
    libraryDependencies ++= Dependencies.streamloaderDependencies
  )

  lazy val mutatorProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Table Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bigquery.mutator.Main"),
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.mutator.generated",
    libraryDependencies ++= Dependencies.mutatorDependencies
  )

  lazy val repeaterProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-repeater",
    description := "Snowplow BigQuery Java app for replaying events from failed inserts subscription",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.repeater.generated",
    libraryDependencies ++= Dependencies.repeaterDependencies
  )

  // TODO: remove?
  lazy val buildInfoSettings = Seq(
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.generated"
  )

  // Make package (build) metadata available within source code.
  lazy val scalifiedSettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
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

  lazy val assemblySettings = Seq(
    assembly / assemblyJarName := { s"${moduleName.value}-${version.value}.jar" },
    assembly / assemblyMergeStrategy := {
      // merge strategy for fixing netty conflict
      case PathList("io", "netty", xs @ _*)                => MergeStrategy.first
      case PathList("org", "hamcrest", xs @ _*)            => MergeStrategy.first
      case PathList("META-INF", "native-image", xs @ _*)   => MergeStrategy.discard
      case PathList("META-INF", "native-image", xs @ _*)   => MergeStrategy.discard
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class")            => MergeStrategy.first
      case x if x.endsWith("git.properties")               => MergeStrategy.first
      case x if x.matches("google/.*\\.proto")             => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { _.data.getName == "activation-1.1.jar" }
    }
  )

  lazy val buildSettings = Seq(
    Global / cancelable := true,
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Dependencies.V.betterMonadicFor),
    addCompilerPlugin(("org.typelevel" %% "kind-projector" % Dependencies.V.kindProjector).cross(CrossVersion.full))
  ) ++ compilerSettings ++ resolverSettings

  val appSettings = buildSettings ++ assemblySettings

  lazy val commonBuildSettings       = commonProjectSettings ++ buildSettings
  lazy val streamloaderBuildSettings = streamloaderProjectSettings ++ appSettings ++ scalifiedSettings ++ macroSettings
  lazy val mutatorBuildSettings      = mutatorProjectSettings ++ appSettings
  lazy val repeaterBuildSettings     = repeaterProjectSettings ++ appSettings ++ scalifiedSettings ++ macroSettings
}
