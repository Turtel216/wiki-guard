package dev.wikiguard.stream.metrics

import java.time.Instant

import io.prometheus.metrics.core.metrics.{Counter, Gauge}
import io.prometheus.metrics.model.registry.PrometheusRegistry

/** The numbers the streaming application reports about its queries. */
final class StreamMetrics(registry: PrometheusRegistry) {

  private val batches = Counter
    .builder()
    .name("wg_stream_batches")
    .help("Batches a query has finished.")
    .labelNames("query")
    .register(registry)

  private val inputRows = Counter
    .builder()
    .name("wg_stream_input_rows")
    .help("Records a query has read from its sources.")
    .labelNames("query")
    .register(registry)

  private val batchDuration = Gauge
    .builder()
    .name("wg_stream_batch_duration_seconds")
    .help("How long the latest batch of a query took from start to commit.")
    .labelNames("query")
    .register(registry)

  private val lastProgress = Gauge
    .builder()
    .name("wg_stream_last_progress_timestamp_seconds")
    .help("When a query last finished a batch or found nothing to do, in seconds since 1970.")
    .labelNames("query")
    .register(registry)

  private val sourceLag = Gauge
    .builder()
    .name("wg_stream_source_lag_records")
    .help("Records waiting in a Kafka topic when the latest batch of a query stopped reading.")
    .labelNames("query", "topic")
    .register(registry)

  def batchFinished(batch: BatchProgress, now: Instant): Unit = {
    batches.labelValues(batch.query).inc()
    inputRows.labelValues(batch.query).inc(batch.inputRows.toDouble)
    batchDuration.labelValues(batch.query).set(batch.duration.toMillis / 1000.0)
    batch.lagByTopic.foreach { case (topic, lag) =>
      sourceLag.labelValues(batch.query, topic).set(lag.toDouble)
    }
    queryAlive(batch.query, now)
  }

  /** The query is running, whether or not it had anything to read. */
  def queryAlive(query: String, now: Instant): Unit =
    lastProgress.labelValues(query).set(now.toEpochMilli / 1000.0)
}
