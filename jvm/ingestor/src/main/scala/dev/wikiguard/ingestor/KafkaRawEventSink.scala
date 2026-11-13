package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.time.{Clock, Duration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import dev.wikiguard.common.kafka.RawTopicNames

/**
 * Stores events in the raw Kafka topics, keyed by page id.
 *
 * Each write waits for the broker to confirm it before the next one starts. Events therefore reach
 * Kafka in stream order, and the newest record is always a safe place to resume from. At a few
 * events per second the wait costs nothing that matters.
 *
 * Records are spread over partitions, so "newest" needs something that orders them across
 * partitions. That is the record timestamp, which this sink sets itself and never repeats.
 *
 * The stream position of an event travels in a record header. That makes Kafka the only place that
 * knows how far the ingestor got.
 *
 * @param newConsumer
 *   opens a consumer for looking up the last written event; it is closed after each lookup
 * @param topics
 *   every raw topic the ingestor writes to
 * @param clock
 *   gives the time that records are stamped with
 */
final class KafkaRawEventSink(
    producer: Producer[String, String],
    newConsumer: () => Consumer[String, String],
    topics: Set[String],
    clock: Clock
) extends RawEventSink
    with AutoCloseable {

  import KafkaRawEventSink.{EventIdHeader, ReadTimeout}
  import RawTopicAccess.{eventId, partitionsOf}

  // The one piece of state in here: the timestamp of the latest record this process wrote.
  private val lastTimestamp = new AtomicLong(0L)

  def write(record: RawRecord): Either[SinkError, Unit] = {
    val message = new ProducerRecord[String, String](
      record.topic,
      KafkaRawEventSink.AnyPartition,
      nextTimestamp(),
      record.pageId.map(_.toString).orNull,
      record.json
    )
    record.eventId.foreach { id =>
      message.headers.add(EventIdHeader, id.getBytes(StandardCharsets.UTF_8))
    }
    Try(producer.send(message).get()).toEither
      .map(_ => ())
      .left
      .map(error => SinkError.WriteFailed(record.topic, error.toString))
  }

  /** The current time, moved forward a millisecond when two writes fall into the same one. */
  private def nextTimestamp(): java.lang.Long =
    lastTimestamp.updateAndGet(last => math.max(last + 1, clock.millis()))

  def lastEvent(): Either[SinkError, Option[ResumePoint]] =
    Try {
      Using.resource(newConsumer()) { consumer =>
        newest(lastRecords(consumer))
      }
    }.toEither.left.map(error => SinkError.ReadFailed(error.toString))

  def close(): Unit = producer.close()

  /** Reads the last record of every partition that has one. */
  private def lastRecords(consumer: Consumer[String, String]) = {
    val partitions = topics.toVector.flatMap(topic => partitionsOf(consumer, topic))
    val starts = consumer.beginningOffsets(partitions.asJava).asScala
    val lastOffsets = consumer
      .endOffsets(partitions.asJava)
      .asScala
      .collect { case (partition, end) if end > starts(partition) => partition -> (end - 1) }
      .toMap
    consumer.assign(lastOffsets.keySet.asJava)
    lastOffsets.foreach { case (partition, offset) => consumer.seek(partition, offset) }
    poll(consumer, lastOffsets, Vector.empty, System.nanoTime() + ReadTimeout.toNanos)
  }

  /** Polls until the last record of every partition has arrived. */
  @tailrec
  private def poll(
      consumer: Consumer[String, String],
      wanted: Map[TopicPartition, Long],
      found: Vector[ConsumerRecord[String, String]],
      deadline: Long
  ): Vector[ConsumerRecord[String, String]] =
    if (found.size >= wanted.size) {
      found
    } else if (System.nanoTime() > deadline) {
      throw new IllegalStateException("timed out reading the last record of each partition")
    } else {
      val batch = consumer.poll(Duration.ofMillis(500)).asScala.toVector.filter { record =>
        wanted.get(new TopicPartition(record.topic, record.partition)).contains(record.offset)
      }
      poll(consumer, wanted, found ++ batch, deadline)
    }

  /** No two records share a timestamp, so the latest one is the newest event. */
  private def newest(records: Vector[ConsumerRecord[String, String]]): Option[ResumePoint] =
    records
      .flatMap(record => eventId(record).map(id => (record, id)))
      .maxByOption { case (record, _) => record.timestamp }
      .map { case (record, id) => ResumePoint(id, eventTime(record)) }

  /** Falls back to the time of writing for an event that does not carry its own time. */
  private def eventTime(record: ConsumerRecord[String, String]): Instant =
    Option(record.value)
      .flatMap(EventEnvelope.parse)
      .flatMap(_.time)
      .getOrElse(Instant.ofEpochMilli(record.timestamp))
}

object KafkaRawEventSink {

  /** Name of the record header that holds the stream position of the event. */
  val EventIdHeader: String = RawTopicNames.EventIdHeader

  private val ReadTimeout = Duration.ofSeconds(30)

  // The Java client takes "no partition" to mean that the key decides.
  private val AnyPartition: Integer = null // scalafix:ok DisableSyntax.null

  def apply(bootstrapServers: String, topics: Set[String]): KafkaRawEventSink = {
    val producer =
      new KafkaProducer(
        producerSettings(bootstrapServers),
        new StringSerializer,
        new StringSerializer
      )
    def newConsumer(): Consumer[String, String] =
      new KafkaConsumer(
        RawTopicAccess.consumerSettings(bootstrapServers, "wikiguard-ingestor-resume"),
        new StringDeserializer,
        new StringDeserializer
      )
    new KafkaRawEventSink(producer, () => newConsumer(), topics, Clock.systemUTC())
  }

  private def producerSettings(bootstrapServers: String): Properties = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(ProducerConfig.CLIENT_ID_CONFIG, "wikiguard-ingestor")
    settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    settings.put(ProducerConfig.ACKS_CONFIG, "all")
    // Every write is waited for, so holding records back to batch them would only add delay.
    settings.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    settings.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd")
    settings
  }
}
