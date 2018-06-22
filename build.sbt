lazy val root = project.in(file("."))
  .settings(Seq(
    name := "snowplow-bigquery-loader",
    description := "Snowplow BigQuery Loader"
  ))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings ++ BuildSettings.noPublishSettings,
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
  ).enablePlugins(JavaAppPackaging)

lazy val mutator = project.in(file("mutator"))
  .settings(Seq(
    name := "snowplow-bigquery-mutator",
    description := "Snowplow BigQuery Mutator",
    mainClass := Some("com.snowplowanalytics.snowplow.storage.bqmutator.Main")
  ))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings ++ BuildSettings.noPublishSettings,
    libraryDependencies ++= Seq(
      Dependencies.pubsub,
      Dependencies.bigQuery,

      Dependencies.decline,
      Dependencies.cats,
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
  ).enablePlugins(JavaAppPackaging)

lazy val repl = project.in(file("repl"))
  .settings(BuildSettings.commonSettings)
  .settings(
    BuildSettings.macroSettings ++ BuildSettings.noPublishSettings,
    description := "Scio REPL for Snowplow BigQueryLoader",
    libraryDependencies ++= Seq(Dependencies.scioRepl),
    Compile / mainClass  := Some("com.spotify.scio.repl.ScioShell")
  ).dependsOn(root)

