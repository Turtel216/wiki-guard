package dev.wikiguard.stream

import org.apache.spark.sql.SparkSession

/** One small local Spark for all tests of this module. */
trait SparkTestSession {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("unit-tests")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
}
