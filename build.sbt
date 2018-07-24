lazy val common = project.in(file("common"))
  .settings(Seq(
    name := "snowplow-bigquery-common",
    description := "Snowplow BigQuery Loader Common Utils"
  ))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings,
    libraryDependencies ++= Seq(
      Dependencies.decline,
      Dependencies.cats,
      Dependencies.analyticsSdk,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.circe,
      Dependencies.circeJavaTime,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )

lazy val root = project.in(file("."))
  .settings(Seq(
    name := "snowplow-bigquery-loader",
    description := "Snowplow BigQuery Loader Dataflow Job"
  ))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings,
    libraryDependencies ++= Seq(
      Dependencies.scioCore,
      Dependencies.decline,
      Dependencies.cats,
      Dependencies.analyticsSdk,
      Dependencies.processingManifest,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.circe,
      Dependencies.circeJavaTime,

      Dependencies.slf4j,
      Dependencies.directRunner,
      Dependencies.dataflowRunner,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)

lazy val mutator = project.in(file("mutator"))
  .settings(Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bigquery.mutator.Main")
  ))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings,
    libraryDependencies ++= Seq(
      Dependencies.pubsub,
      Dependencies.bigQuery,

      Dependencies.fs2,
      Dependencies.decline,
      Dependencies.cats,
      Dependencies.catsEffect,
      Dependencies.analyticsSdk,
      Dependencies.processingManifest,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.circe,
      Dependencies.circeJavaTime,
      Dependencies.schemaDdl,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)

