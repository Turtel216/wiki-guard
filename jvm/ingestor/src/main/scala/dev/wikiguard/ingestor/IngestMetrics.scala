package dev.wikiguard.ingestor

import java.time.{Duration, Instant}

import io.prometheus.metrics.core.metrics.{Counter, Gauge}
import io.prometheus.metrics.model.registry.PrometheusRegistry

/** The numbers the ingestor reports about itself. */
final class IngestMetrics(registry: PrometheusRegistry) {

  private val received = Counter
    .builder()
    .name("wg_ingestor_events_received")
    .help("Events read from the source, before any filtering.")
    .register(registry)

  private val written = Counter
    .builder()
    .name("wg_ingestor_events_written")
    .help("Events stored in a raw topic.")
    .labelNames("topic")
    .register(registry)

  private val dropped = Counter
    .builder()
    .name("wg_ingestor_events_dropped")
    .help("Events that were read but not stored, by reason.")
    .labelNames("reason")
    .register(registry)

  private val connections = Counter
    .builder()
    .name("wg_ingestor_connections")
    .help("Connections to the source that have ended, by how they ended.")
    .labelNames("outcome")
    .register(registry)

  private val gaps = Counter
    .builder()
    .name("wg_ingestor_gaps")
    .help("Times the ingestor resumed too late and lost events for good.")
    .register(registry)

  private val lastWrite = Gauge
    .builder()
    .name("wg_ingestor_last_write_timestamp_seconds")
    .help("When the latest event was stored, in seconds since 1970.")
    .register(registry)

  private val eventDelay = Gauge
    .builder()
    .name("wg_ingestor_event_delay_seconds")
    .help("How old the latest stored event was at the moment it was stored.")
    .register(registry)

  def eventReceived(): Unit = received.inc()

  def eventDropped(reason: DropReason): Unit = dropped.labelValues(reason.label).inc()

  def eventWritten(topic: String, now: Instant, eventTime: Option[Instant]): Unit = {
    written.labelValues(topic).inc()
    lastWrite.set(now.toEpochMilli / 1000.0)
    eventTime.foreach(time => eventDelay.set(Duration.between(time, now).toMillis / 1000.0))
  }

  /** The outcome is a short fixed word such as "closed" or "failed". */
  def connectionEnded(outcome: String): Unit = connections.labelValues(outcome).inc()

  def gapFound(): Unit = gaps.inc()
}
