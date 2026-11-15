package dev.wikiguard.stream.metrics

import java.nio.file.{Files, Path}
import java.time.{Clock, Instant, ZoneOffset}
import java.util.Comparator

import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import dev.wikiguard.stream.SparkTestSession

class MetricsListenerTest extends AnyFunSuite with SparkTestSession with Eventually {

  // Spark hands events to listeners on a thread of its own, a moment after a batch ends.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(100, Millis))

  private val now = Instant.parse("2026-11-16T12:00:00Z")
  private val clock = Clock.fixed(now, ZoneOffset.UTC)
  private val LastProgress = "wg_stream_last_progress_timestamp_seconds"

  /**
   * Runs a query with this name over a directory holding three lines of text. The check runs after
   * the lines have been read and while the query is still up.
   */
  private def copyLines[A](queryName: String, listener: MetricsListener)(check: => A): A = {
    val input = Files.createTempDirectory("listener-input")
    val checkpoint = Files.createTempDirectory("listener-checkpoint")
    Files.writeString(input.resolve("lines.txt"), "one\ntwo\nthree\n")
    // A query with nothing to read says so this often. The default is too slow for a test.
    spark.conf.set("spark.sql.streaming.noDataProgressEventInterval", "200")
    spark.streams.addListener(listener)
    try {
      val query = spark.readStream
        .text(input.toString)
        .writeStream
        .format("noop")
        .queryName(queryName)
        .option("checkpointLocation", checkpoint.toString)
        .start()
      try {
        query.processAllAvailable()
        check
      } finally query.stop()
    } finally {
      spark.streams.removeListener(listener)
      deleteAll(checkpoint)
      deleteAll(input)
    }
  }

  private def deleteAll(directory: Path): Unit = {
    val paths = Files.walk(directory)
    try paths.sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))
    finally paths.close()
  }

  test("a batch of a running query shows up under the name of the query") {
    val registry = new PrometheusRegistry
    val listener = new MetricsListener(new StreamMetrics(registry), _ => None, clock)
    val query = "query" -> "copy-lines"

    copyLines("copy-lines", listener) {
      eventually {
        assert(MetricValues.value(registry, "wg_stream_input_rows", query).contains(3.0))
      }
    }

    assert(MetricValues.value(registry, "wg_stream_batches", query).exists(_ >= 1.0))
    assert(
      MetricValues.value(registry, LastProgress, query).contains(now.getEpochSecond.toDouble)
    )
    // A directory of files is not a Kafka topic, so there is no lag to report.
    assert(MetricValues.value(registry, "wg_stream_source_lag_records", query).isEmpty)
  }

  test("a query with nothing left to read is looked up by its id and marked as alive") {
    val registry = new PrometheusRegistry
    // The event for an idle query carries no name, so this name can only come from the lookup.
    val listener = new MetricsListener(new StreamMetrics(registry), _ => Some("looked-up"), clock)

    copyLines("copy-lines", listener) {
      eventually {
        assert(
          MetricValues.value(registry, LastProgress, "query" -> "looked-up")
            .contains(now.getEpochSecond.toDouble)
        )
      }
    }
  }

  test("an idle query that cannot be found does no harm") {
    val registry = new PrometheusRegistry
    val listener = new MetricsListener(new StreamMetrics(registry), _ => None, clock)

    copyLines("copy-lines", listener) {
      eventually {
        assert(MetricValues.value(
          registry,
          "wg_stream_input_rows",
          "query" -> "copy-lines"
        ).nonEmpty)
      }
      // Long enough for several idle events to arrive.
      Thread.sleep(1000)
    }

    assert(MetricValues.value(registry, LastProgress, "query" -> "copy-lines").nonEmpty)
  }
}
