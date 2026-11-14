package dev.wikiguard.integration

import org.apache.spark.sql.SparkSession
import org.testcontainers.postgresql.PostgreSQLContainer

/**
 * A local Spark wired to the lake the way production is: SeaweedFS for files, PostgreSQL for the
 * catalog.
 */
object LakeSpark {

  /** The name under which the session knows the Iceberg catalog. */
  val Catalog: String = "lake"

  /**
   * Both containers must be running. Hadoop keeps one connection per bucket name for the whole JVM,
   * so every test class needs bucket names of its own.
   */
  def session(
      appName: String,
      postgres: PostgreSQLContainer,
      seaweedFs: SeaweedFsContainer,
      bucket: String
  ): SparkSession =
    SparkSession
      .builder()
      .master("local[2]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(s"spark.sql.catalog.$Catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$Catalog.type", "jdbc")
      .config(s"spark.sql.catalog.$Catalog.uri", postgres.getJdbcUrl)
      .config(s"spark.sql.catalog.$Catalog.jdbc.user", postgres.getUsername)
      .config(s"spark.sql.catalog.$Catalog.jdbc.password", postgres.getPassword)
      .config(s"spark.sql.catalog.$Catalog.jdbc.schema-version", "V1")
      .config(s"spark.sql.catalog.$Catalog.warehouse", s"s3a://$bucket/warehouse")
      .config(s"spark.sql.catalog.$Catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
      .config("spark.hadoop.fs.s3a.endpoint", seaweedFs.endpoint)
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.access.key", SeaweedFsContainer.AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", SeaweedFsContainer.SecretKey)
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      .getOrCreate()

  def requiredEnv(name: String): String =
    sys.env.getOrElse(name, sys.error(s"$name is not set; run this test through sbt"))
}
