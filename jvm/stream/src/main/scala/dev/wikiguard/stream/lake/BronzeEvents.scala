package dev.wikiguard.stream.lake

import org.apache.spark.sql.{functions => F, Column, DataFrame}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import dev.wikiguard.common.kafka.RawTopicNames

/**
 * Turns records of the raw Kafka topics into rows of bronze.events_raw.
 *
 * The body is kept as it is. Only the few fields that the table is laid out by are read from it,
 * and a body that cannot be read still gets a row, so nothing that reached Kafka is ever lost here.
 */
object BronzeEvents {

  private val Envelope = "envelope"

  // The only parts of an event body this step depends on.
  private val EnvelopeSchema = StructType(
    Seq(
      StructField(
        "meta",
        StructType(Seq(StructField("stream", StringType), StructField("dt", StringType)))
      ),
      StructField("database", StringType)
    )
  )

  /** Expects the records to carry their headers. A record without a value is not an event. */
  def fromKafka(records: DataFrame): DataFrame =
    records
      .filter(F.col(KafkaCols.Value).isNotNull)
      .withColumn(BronzeCols.Json, F.col(KafkaCols.Value).cast(StringType))
      .withColumn(Envelope, F.from_json(F.col(BronzeCols.Json), EnvelopeSchema))
      .select(
        F.col(s"$Envelope.database").as(BronzeCols.Wiki),
        F.col(s"$Envelope.meta.stream").as(BronzeCols.Stream),
        eventId.as(BronzeCols.EventId),
        eventTime.as(BronzeCols.EventTs),
        F.col(KafkaCols.Timestamp).as(BronzeCols.IngestedAt),
        F.col(BronzeCols.Json),
        F.col(KafkaCols.Topic).as(BronzeCols.KafkaTopic),
        F.col(KafkaCols.Partition).as(BronzeCols.KafkaPartition),
        F.col(KafkaCols.Offset).as(BronzeCols.KafkaOffset)
      )

  /** The stream position that the ingestor stored in a record header. The last one wins. */
  private def eventId: Column = {
    val matching = F.filter(
      F.col(KafkaCols.Headers),
      header => header.getField(KafkaCols.HeaderKey) === RawTopicNames.EventIdHeader
    )
    F.try_element_at(matching, F.lit(-1)).getField(KafkaCols.HeaderValue).cast(StringType)
  }

  /**
   * The table is partitioned by this time, so it must never be empty. An event without a readable
   * time of its own is filed under the time it was ingested.
   */
  private def eventTime: Column =
    F.coalesce(F.try_to_timestamp(F.col(s"$Envelope.meta.dt")), F.col(KafkaCols.Timestamp))
}
