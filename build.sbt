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
      Dependencies.json4sExt,
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
    description := "Snowplow BigQuery Loader Dataflow Job"
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
  .dependsOn(common)

lazy val mutator = project.in(file("mutator"))
  .settings(Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Table Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bigquery.mutator.Main")
  ))
  .settings(BuildSettings.dockerSettings)
  .settings(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    BuildSettings.commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.pubsub,
      Dependencies.bigQuery,

      Dependencies.fs2,
      Dependencies.catsEffect,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)

