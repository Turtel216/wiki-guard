package dev.wikiguard.integration

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Proves that the pinned Spark, Iceberg and Hadoop versions work together against the storage we
 * run: table data on SeaweedFS through S3, the catalog in PostgreSQL.
 */
class IcebergSmokeTest extends AnyFunSuite with BeforeAndAfterAll {

  import LakeSpark.requiredEnv

  private val Bucket = "wg-lake-smoke"
  private val Namespace = s"${LakeSpark.Catalog}.smoke"

  private val seaweedFs = SeaweedFsContainer(requiredEnv("WG_SEAWEEDFS_IMAGE"))

  private val postgres =
    new PostgreSQLContainer(DockerImageName.parse(requiredEnv("WG_POSTGRES_IMAGE")))
      .withDatabaseName("iceberg")

  private lazy val spark: SparkSession =
    LakeSpark.session("iceberg-smoke-test", postgres, seaweedFs, Bucket)

  private val firstDay = Vector(
    LakeEvent("enwiki", 1L, Instant.parse("2026-11-01T10:00:00Z"), 0.10),
    LakeEvent("enwiki", 2L, Instant.parse("2026-11-01T23:59:59Z"), 0.75)
  )

  private val secondDay = Vector(
    LakeEvent("enwiki", 3L, Instant.parse("2026-11-02T00:00:00Z"), 0.40)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    seaweedFs.start()
    postgres.start()
    seaweedFs.createBucket(Bucket)
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $Namespace").collect()
    ()
  }

  override def afterAll(): Unit =
    try spark.stop()
    finally {
      postgres.stop()
      seaweedFs.stop()
      super.afterAll()
    }

  test("rows appended to a table read back unchanged") {
    import spark.implicits._
    val table = createTable("roundtrip")

    append(table, firstDay ++ secondDay)

    val readBack = spark.table(table).as[LakeEvent].collect().toSet
    assert(readBack == (firstDay ++ secondDay).toSet)
  }

  test("data files are Parquet in the bucket, one partition per day") {
    import spark.implicits._
    val table = createTable("layout")

    append(table, firstDay ++ secondDay)

    val files = spark
      .sql(s"SELECT file_path, file_format, CAST(partition.rev_ts_day AS STRING) FROM $table.files")
      .as[(String, String, String)]
      .collect()
      .toVector
    assert(files.nonEmpty)
    assert(files.forall { case (path, _, _) => path.startsWith(s"s3a://$Bucket/warehouse/") })
    assert(files.forall { case (_, format, _) => format == "PARQUET" })
    assert(files.map { case (_, _, day) => day }.toSet == Set("2026-11-01", "2026-11-02"))
  }

  test("MERGE updates matching rows and inserts new ones") {
    import spark.implicits._
    val table = createTable("merge")
    append(table, firstDay)
    val rescored = LakeEvent("enwiki", 2L, Instant.parse("2026-11-01T23:59:59Z"), 0.95)
    (rescored +: secondDay).toDS().createOrReplaceTempView("updates")

    spark
      .sql(s"""MERGE INTO $table t USING updates u ON t.rev_id = u.rev_id
              |WHEN MATCHED THEN UPDATE SET *
              |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      .collect()

    val merged = spark.table(table).as[LakeEvent].collect().toSet
    assert(merged == firstDay.filter(_.rev_id == 1L).toSet + rescored ++ secondDay)
  }

  test("an earlier snapshot stays readable after another append") {
    import spark.implicits._
    val table = createTable("snapshots")
    append(table, firstDay)
    append(table, secondDay)

    val snapshotIds = spark
      .sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at")
      .as[Long]
      .collect()
      .toVector
    assert(snapshotIds.length == 2)

    val firstSnapshotRows = snapshotIds.headOption.map { id =>
      spark.sql(s"SELECT * FROM $table VERSION AS OF $id").as[LakeEvent].collect().toSet
    }
    assert(firstSnapshotRows.contains(firstDay.toSet))
  }

  /** Creates an empty table laid out the way the lake tables are, and returns its name. */
  private def createTable(name: String): String = {
    val table = s"$Namespace.$name"
    spark
      .sql(s"""CREATE TABLE $table (wiki STRING, rev_id BIGINT, rev_ts TIMESTAMP, score DOUBLE)
              |USING iceberg
              |PARTITIONED BY (days(rev_ts))
              |TBLPROPERTIES (
              |  'format-version' = '2',
              |  'write.parquet.compression-codec' = 'zstd'
              |)""".stripMargin)
      .collect()
    table
  }

  private def append(table: String, events: Vector[LakeEvent]): Unit = {
    import spark.implicits._
    events.toDS().writeTo(table).append()
  }
}
