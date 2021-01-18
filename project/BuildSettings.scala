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
import com.typesafe.sbt.packager
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer, packageName}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.ExecCmd
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

object BuildSettings {
  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    version := "0.6.1",
    scalaVersion := "2.13.2",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description, BuildInfoKey.action("userAgent") {
      s"${name.value}/${version.value}"
    })
  )

  lazy val commonProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-common",
    description := "Snowplow BigQuery Loader Common Utils",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.common.generated"
  )

  lazy val loaderProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-loader",
    description := "Snowplow BigQuery Loader Dataflow Job",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.loader.generated"
  )

  lazy val streamloaderProjectSettings = projectSettings ++ Seq(
    name := "snowplow-bigquery-streamloader",
    description := "Snowplow BigQuery Loader Standalone App",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.streamloader.generated"
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
    sourceDirectory in Universal := new java.io.File((baseDirectory in LocalRootProject).value, "docker"),
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.2.1",
    daemonUser in Docker := "root",
    packageName in Docker := s"${name.value}",
    dockerUsername := Some("snowplow"),
    dockerUpdateLatest := true,
    dockerEnvVars := Map("SNOWPLOW_BIGQUERY_APP" -> name.value),
    dockerCommands += ExecCmd("RUN", "cp", "/opt/docker/bin/docker-entrypoint.sh", "/usr/local/bin/"),
    dockerEntrypoint := Seq("docker-entrypoint.sh"),
    dockerCmd := Seq("--help")
  )

  lazy val assemblySettings = Seq(
    assemblyJarName in assembly := { s"${moduleName.value}-${version.value}.jar" },
    assemblyMergeStrategy in assembly := {
      // merge strategy for fixing netty conflict
      case PathList("io", "netty", xs @ _*)                => MergeStrategy.first
      case PathList("META-INF", "native-image", xs @ _*)   => MergeStrategy.discard
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class")            => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp.filter { _.data.getName == "activation-1.1.jar" }
    }
  )

  lazy val buildSettings = Seq(
    Global / cancelable := true,
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Dependencies.V.betterMonadicFor),
    addCompilerPlugin(("org.typelevel" %% "kind-projector" % Dependencies.V.kindProjector).cross(CrossVersion.full))
  ) ++ compilerSettings ++ resolverSettings ++ dockerSettings ++ assemblySettings

  lazy val commonBuildSettings       = (commonProjectSettings ++ buildSettings).diff(dockerSettings).diff(assemblySettings)
  lazy val loaderBuildSettings       = loaderProjectSettings ++ buildSettings ++ scalifiedSettings ++ macroSettings
  lazy val streamloaderBuildSettings = streamloaderProjectSettings ++ buildSettings ++ scalifiedSettings ++ macroSettings ++ assemblySettings
  lazy val mutatorBuildSettings      = mutatorProjectSettings ++ buildSettings ++ assemblySettings
  lazy val repeaterBuildSettings     = repeaterProjectSettings ++ buildSettings ++ scalifiedSettings ++ macroSettings ++ assemblySettings
  lazy val forwarderBuildSettings    = forwarderProjectSettings ++ buildSettings ++ macroSettings
}
