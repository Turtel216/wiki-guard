ThisBuild / organization := "dev.wikiguard"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Versions("jvm.scala")

ThisBuild / scalacOptions ++= Seq(
  "-release",
  Versions("jvm.java"),
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Wdead-code",
  "-Wvalue-discard",
  "-Wnumeric-widen",
  "-Wunused:imports,privates,locals,implicits,nowarn"
)

// Warnings only fail the build in CI, so local iteration is not blocked by them.
ThisBuild / scalacOptions ++= (if (sys.env.contains("CI")) Seq("-Werror") else Nil)

// scalafix needs SemanticDB output for its semantic rules.
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

lazy val scalatest = "org.scalatest" %% "scalatest" % Versions("jvm.scalatest") % Test

// Spark is provided by the runtime image and must not end up in our jars.
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % Versions("jvm.spark") % Provided

lazy val commonSettings = Seq(
  libraryDependencies += scalatest,
  Test / fork := true
)

lazy val root = (project in file("."))
  .aggregate(common, featuresCore, ingestor, enricher, stream, batch)
  .settings(
    name := "wikiguard",
    publish / skip := true
  )

// Generated protos, config, metrics, Kafka and S3 helpers.
lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(name := "wikiguard-common")

// Pure feature functions. This module must never depend on Spark.
lazy val featuresCore = (project in file("features-core"))
  .settings(commonSettings)
  .settings(name := "wikiguard-features-core")

// EventStreams to Kafka.
lazy val ingestor = (project in file("ingestor"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(name := "wikiguard-ingestor")

// Rate-limited Wikimedia API client.
lazy val enricher = (project in file("enricher"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(name := "wikiguard-enricher")

// Structured Streaming queries and state processors.
lazy val stream = (project in file("stream"))
  .dependsOn(common, featuresCore)
  .settings(commonSettings)
  .settings(
    name := "wikiguard-stream",
    libraryDependencies += sparkSql
  )

// Backfill, label finalization, evaluation and table maintenance.
lazy val batch = (project in file("batch"))
  .dependsOn(common, featuresCore)
  .settings(commonSettings)
  .settings(
    name := "wikiguard-batch",
    libraryDependencies += sparkSql
  )
