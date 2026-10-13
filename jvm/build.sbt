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

lazy val jackson = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % Versions("jvm.jackson"),
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions("jvm.jackson")
)

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % Versions("jvm.kafka")

lazy val slf4jApi = "org.slf4j" % "slf4j-api" % Versions("jvm.slf4j")

// The log4j backend and its JSON layout. Only needed when a service runs.
lazy val log4jRuntime = Seq("log4j-core", "log4j-slf4j2-impl", "log4j-layout-template-json")
  .map(module => "org.apache.logging.log4j" % module % Versions("jvm.log4j") % Runtime)

lazy val prometheus = Seq(
  "prometheus-metrics-core",
  "prometheus-metrics-exporter-httpserver",
  "prometheus-metrics-instrumentation-jvm"
).map(module => "io.prometheus" % module % Versions("jvm.prometheus"))

// Spark is provided by the runtime image and must not end up in our jars.
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % Versions("jvm.spark") % Provided

// Must be the same version as the Python xgboost package that trains the models.
lazy val xgboost4j = "ml.dmlc" %% "xgboost4j" % Versions("shared.xgboost")

// Spark reaches into JDK internals, which newer Java versions only allow when asked.
// spark-submit adds these flags itself; forked test JVMs need them spelled out.
lazy val sparkJavaOptions = Seq(
  "java.base/java.lang",
  "java.base/java.lang.invoke",
  "java.base/java.lang.reflect",
  "java.base/java.io",
  "java.base/java.net",
  "java.base/java.nio",
  "java.base/java.util",
  "java.base/java.util.concurrent",
  "java.base/java.util.concurrent.atomic",
  "java.base/jdk.internal.ref",
  "java.base/sun.nio.ch",
  "java.base/sun.nio.cs",
  "java.base/sun.security.action",
  "java.base/sun.util.calendar"
).map(pkg => s"--add-opens=$pkg=ALL-UNNAMED")

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
  .settings(
    name := "wikiguard-common",
    libraryDependencies ++= jackson
  )

// Pure feature functions. This module must never depend on Spark.
lazy val featuresCore = (project in file("features-core"))
  .settings(commonSettings)
  .settings(name := "wikiguard-features-core")

// EventStreams to Kafka.
lazy val ingestor = (project in file("ingestor"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    name := "wikiguard-ingestor",
    libraryDependencies ++= Seq(kafkaClients, slf4jApi) ++ log4jRuntime ++ prometheus
  )

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
    libraryDependencies ++= Seq(sparkSql, xgboost4j)
  )

// Backfill, label finalization, evaluation and table maintenance.
lazy val batch = (project in file("batch"))
  .dependsOn(common, featuresCore)
  .settings(commonSettings)
  .settings(
    name := "wikiguard-batch",
    libraryDependencies += sparkSql
  )

// Tests that need Docker. This project is left out of the root aggregate on purpose,
// so a plain `sbt test` never starts containers. Run it with `sbt integration/test`.
lazy val integration = (project in file("integration"))
  .dependsOn(ingestor % "test->test")
  .settings(commonSettings)
  .settings(
    name := "wikiguard-integration",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % Versions("jvm.spark") % Test,
      "org.apache.iceberg" %% "iceberg-spark-runtime-4.1" % Versions("jvm.iceberg") % Test,
      "org.apache.hadoop" % "hadoop-aws" % Versions("jvm.hadoop") % Test,
      "org.postgresql" % "postgresql" % Versions("jvm.postgres_jdbc") % Test,
      "org.testcontainers" % "testcontainers" % Versions("jvm.testcontainers") % Test,
      "org.testcontainers" % "testcontainers-postgresql" % Versions("jvm.testcontainers") % Test,
      "org.testcontainers" % "testcontainers-kafka" % Versions("jvm.testcontainers") % Test
    ),
    Test / javaOptions ++= sparkJavaOptions,
    Test / envVars ++= Map(
      "WG_SEAWEEDFS_IMAGE" -> Versions("images.seaweedfs"),
      "WG_POSTGRES_IMAGE" -> Versions("images.postgres"),
      "WG_KAFKA_IMAGE" -> Versions("images.kafka")
    )
  )
