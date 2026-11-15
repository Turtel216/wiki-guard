package dev.wikiguard.stream.metrics

import java.time.Instant

import scala.concurrent.duration.DurationInt

import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.scalatest.funsuite.AnyFunSuite

class StreamMetricsTest extends AnyFunSuite {

  private val now = Instant.parse("2026-11-16T12:00:00Z")

  private def batch(rows: Long, lag: Map[String, Long] = Map.empty): BatchProgress =
    BatchProgress("lake-sink", rows, 1500.millis, lag)

  test("batches and their rows add up per query") {
    val registry = new PrometheusRegistry
    val metrics = new StreamMetrics(registry)

    metrics.batchFinished(batch(rows = 40), now)
    metrics.batchFinished(batch(rows = 2), now)
    metrics.batchFinished(batch(rows = 7).copy(query = "normalize"), now)

    val query = "query" -> "lake-sink"
    assert(MetricValues.value(registry, "wg_stream_batches", query).contains(2.0))
    assert(MetricValues.value(registry, "wg_stream_input_rows", query).contains(42.0))
    assert(
      MetricValues.value(registry, "wg_stream_input_rows", "query" -> "normalize").contains(7.0)
    )
  }

  test("the duration, the lag and the time are those of the latest batch") {
    val registry = new PrometheusRegistry
    val metrics = new StreamMetrics(registry)

    metrics.batchFinished(batch(rows = 5, lag = Map("a" -> 90L, "b" -> 3L)), now)
    metrics.batchFinished(batch(rows = 5, lag = Map("a" -> 10L, "b" -> 0L)), now.plusSeconds(60))

    val query = "query" -> "lake-sink"
    assert(MetricValues.value(registry, "wg_stream_batch_duration_seconds", query).contains(1.5))
    assert(
      MetricValues.value(registry, "wg_stream_source_lag_records", query, "topic" -> "a")
        .contains(10.0)
    )
    assert(
      MetricValues.value(registry, "wg_stream_source_lag_records", query, "topic" -> "b")
        .contains(0.0)
    )
    assert(
      MetricValues.value(registry, "wg_stream_last_progress_timestamp_seconds", query)
        .contains(now.plusSeconds(60).getEpochSecond.toDouble)
    )
  }

  test("a query with nothing to read still shows that it is alive") {
    val registry = new PrometheusRegistry
    val metrics = new StreamMetrics(registry)

    metrics.queryAlive("lake-sink", now)

    val query = "query" -> "lake-sink"
    assert(
      MetricValues.value(registry, "wg_stream_last_progress_timestamp_seconds", query)
        .contains(now.getEpochSecond.toDouble)
    )
    assert(MetricValues.value(registry, "wg_stream_batches", query).isEmpty)
  }
}
