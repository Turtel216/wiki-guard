package dev.wikiguard.ingestor

import java.time.{Clock, Instant}
import java.util.Properties

import scala.util.Try

import com.google.protobuf.Timestamp
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import dev.wikiguard.contracts.v1.{IngestGap => IngestGapMessage}

/**
 * Publishes gaps to Kafka, from where they are copied into the lake.
 *
 * The stream is shared by all wikis, so a gap in it is a gap for each of them. One message is
 * published per wiki, keyed by wiki.
 *
 * @param wikis
 *   the wikis the ingestor follows
 * @param clock
 *   gives the time a gap is reported as detected
 */
final class KafkaGapRecorder(
    producer: Producer[String, Array[Byte]],
    topic: String,
    wikis: Set[String],
    clock: Clock
) extends GapRecorder
    with AutoCloseable {

  def record(gap: IngestGap): Either[SinkError, Unit] = {
    val detectedAt = clock.instant()
    val failures = wikis.toVector.sorted.flatMap { wiki =>
      val message = KafkaGapRecorder.message(wiki, gap, detectedAt)
      val sent = Try(producer.send(new ProducerRecord(topic, wiki, message.toByteArray)).get())
      sent.failed.toOption
    }
    failures.headOption
      .map(error => SinkError.WriteFailed(topic, error.toString))
      .toLeft(())
  }

  def close(): Unit = producer.close()
}

object KafkaGapRecorder {

  def apply(bootstrapServers: String, wikis: Set[String]): KafkaGapRecorder = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(ProducerConfig.CLIENT_ID_CONFIG, "wikiguard-ingestor-gaps")
    settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    settings.put(ProducerConfig.ACKS_CONFIG, "all")
    val producer = new KafkaProducer(settings, new StringSerializer, new ByteArraySerializer)
    new KafkaGapRecorder(producer, OpsTopics.IngestGaps, wikis, Clock.systemUTC())
  }

  def message(wiki: String, gap: IngestGap, detectedAt: Instant): IngestGapMessage =
    IngestGapMessage
      .newBuilder()
      .setWiki(wiki)
      .setGapStart(timestamp(gap.from))
      .setGapEnd(timestamp(gap.to))
      .setDetectedAt(timestamp(detectedAt))
      .build()

  private def timestamp(instant: Instant): Timestamp =
    Timestamp.newBuilder().setSeconds(instant.getEpochSecond).setNanos(instant.getNano).build()
}
