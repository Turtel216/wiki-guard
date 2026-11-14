package dev.wikiguard.stream.lake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import dev.wikiguard.common.lake.LakeTables
import dev.wikiguard.stream.StreamConfig

/**
 * The query that copies Kafka topics into the lake.
 *
 * For now it only knows the raw topics, which all go to bronze.events_raw.
 *
 * A batch is committed to the table together with its number. When a batch is run again after a
 * crash, the table recognises it and takes it only once, so rows are neither lost nor doubled as
 * long as the checkpoint survives.
 */
object LakeSink {

  val QueryName: String = "lake-sink"

  /**
   * Starts the query and returns at once.
   *
   * @param rawTopics
   *   the Kafka topics that hold upstream events as received
   */
  def start(spark: SparkSession, config: StreamConfig, rawTopics: Set[String]): StreamingQuery =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", rawTopics.toVector.sorted.mkString(","))
      // Only matters on the very first start: take what is already in the topics too.
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .option("maxOffsetsPerTrigger", config.maxOffsetsPerTrigger)
      .load()
      .transform(BronzeEvents.fromKafka)
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .queryName(QueryName)
      .trigger(Trigger.ProcessingTime(config.lakeSinkTrigger))
      .option("checkpointLocation", config.checkpointOf(QueryName))
      // A batch can hold events of more than one day. This lets a task keep a file open per
      // day instead of asking for the rows to be sorted by day first.
      .option("fanout-enabled", "true")
      .toTable(s"${config.catalog}.${LakeTables.BronzeEventsRaw}")
}
