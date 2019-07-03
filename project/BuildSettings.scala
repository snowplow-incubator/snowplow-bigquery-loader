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
import Keys._
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.ExecCmd
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._


object BuildSettings {
  lazy val commonSettings = Seq(
    organization          := "com.snowplowanalytics",
    version               := "0.2.0-rc9",
    scalaVersion          := "2.12.8",
    scalacOptions         ++= Seq("-target:jvm-1.8",
      "-language:existentials",
      "-language:higherKinds",
      "-deprecation",
      "-feature",
      "-unchecked"),
    javacOptions          ++= Seq("-source", "1.8", "-target", "1.8"),
    resolvers             += "Snowplow Bintray" at "https://snowplow.bintray.com/snowplow-maven/",
    Global / cancelable   := true,

    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Dependencies.V.betterMonadicFor),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % Dependencies.V.kindProjector),

    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description,
      BuildInfoKey.action("userAgent") { s"${name.value}/${version.value}" }
    )
  )

  lazy val buildInfo = Seq(
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.generated"
  )

  lazy val macroSettings = Seq(
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    addCompilerPlugin("org.scalamacros" % "paradise" % Dependencies.V.scalaMacrosVersion cross CrossVersion.full)
  )

  lazy val dockerSettings = Seq(
    // Use single entrypoint script for all apps
    Universal / sourceDirectory := new java.io.File((baseDirectory in LocalRootProject).value, "docker"),
    dockerRepository := Some("snowplow-docker-registry.bintray.io"),
    dockerUsername := Some("snowplow"),
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    Docker / daemonUser := "root",  // Will be gosu'ed by docker-entrypoint.sh
    dockerEnvVars := Map("SNOWPLOW_BIGQUERY_APP" -> name.value),
    dockerCommands += ExecCmd("RUN", "cp", "/opt/docker/bin/docker-entrypoint.sh", "/usr/local/bin/"),
    dockerEntrypoint := Seq("docker-entrypoint.sh"),
    dockerCmd := Seq("--help")
  )

  // Makes package (build) metadata available withing source code
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(file, """package %s
                       |object ProjectMetadata {
                       |  val version = "%s"
                       |  val name = "%s"
                       |  val organization = "%s"
                       |  val scalaVersion = "%s"
                       |}
                       |""".stripMargin.format(buildInfoPackage.value, version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )
}
