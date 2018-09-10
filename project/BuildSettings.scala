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

import com.typesafe.sbt.packager.Keys.{ maintainer, daemonUser}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

object BuildSettings {
  lazy val commonSettings = Seq(
    organization          := "com.snowplowanalytics",
    version               := "0.1.0-rc13",
    scalaVersion          := "2.11.12",
    scalacOptions         ++= Seq("-target:jvm-1.8",
      "-deprecation",
      "-feature",
      "-unchecked"),
    javacOptions          ++= Seq("-source", "1.8", "-target", "1.8"),
    resolvers             += "Snowplow Bintray" at "https://snowplow.bintray.com/snowplow-maven/",
    Global / cancelable   := true,

    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Dependencies.V.betterMonadicFor),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % Dependencies.V.kindProjector)
  )

  lazy val macroSettings = Seq(
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    addCompilerPlugin("org.scalamacros" % "paradise" % Dependencies.V.scalaMacrosVersion cross CrossVersion.full)
  )

  val dockerSettings = Seq(
    dockerRepository := Some("snowplow-docker-registry.bintray.io"),
    dockerUsername := Some("snowplow"),
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    daemonUser in Docker := "snowplow"
  )
}
