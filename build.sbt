lazy val snowplowBigqueryLoader = project.in(file("."))
  .settings(BuildSettings.commonSettings)

lazy val common = project.in(file("common"))
  .settings(Seq(
    name := "snowplow-bigquery-common",

    description := "Snowplow BigQuery Loader Common Utils"
  ))
  .settings(
    BuildSettings.commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.decline,
      Dependencies.cats,
      Dependencies.analyticsSdk,
      Dependencies.schemaDdl,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.circe,
      Dependencies.circeJavaTime,
      Dependencies.circeParser,

      Dependencies.bigQuery,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )

lazy val loader = project.in(file("loader"))
  .settings(Seq(
    name := "snowplow-bigquery-loader",
    description := "Snowplow BigQuery Loader Dataflow Job",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.loader.generated"
  ))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.dockerSettings)
  .settings(
    BuildSettings.commonSettings,
    BuildSettings.macroSettings,
    libraryDependencies ++= Seq(
      Dependencies.scioCore,

      Dependencies.slf4j,
      Dependencies.directRunner,
      Dependencies.dataflowRunner,

      Dependencies.circeLiteral,
      Dependencies.circeJawn,
      Dependencies.specs2,
      Dependencies.scioTest,
      Dependencies.scalaCheck
    )
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)

lazy val mutator = project.in(file("mutator"))
  .settings(Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Table Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bigquery.mutator.Main"),
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.mutator.generated"
  ))
  .settings(BuildSettings.dockerSettings)
  .settings(
    BuildSettings.commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.pubsub,
      Dependencies.bigQuery,
      Dependencies.igluClient,

      Dependencies.fs2,
      Dependencies.catsEffect,

      Dependencies.specs2,
      Dependencies.scalaCheck,
      Dependencies.circeLiteral
    )
  )
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)

lazy val forwarder = project.in(file("forwarder"))
  .settings(Seq(
    name := "snowplow-bigquery-forwarder",
    description := "Snowplow BigQuery Loader Dataflow Job",
    buildInfoPackage := "com.snowplowanalytics.snowplow.storage.bigquery.forwarder.generated"
  ))
  .settings(BuildSettings.dockerSettings)
  .settings(
    BuildSettings.commonSettings,
    BuildSettings.macroSettings,
    libraryDependencies ++= Seq(
      Dependencies.scioCore,

      Dependencies.slf4j,
      Dependencies.directRunner,
      Dependencies.dataflowRunner,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(common)
