package dev.wikiguard.stream.metrics

import java.time.Clock
import java.util.UUID

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryIdleEvent,
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}

/**
 * Turns what Spark tells the driver about its streaming queries into metrics.
 *
 * @param nameOf
 *   finds the name of a running query. Spark leaves the name out of some of its events.
 */
final class MetricsListener(
    metrics: StreamMetrics,
    nameOf: UUID => Option[String],
    clock: Clock
) extends StreamingQueryListener {

  override def onQueryStarted(event: QueryStartedEvent): Unit =
    Option(event.name).foreach(query => metrics.queryAlive(query, clock.instant()))

  override def onQueryProgress(event: QueryProgressEvent): Unit =
    BatchProgress.from(event.progress).foreach(batch =>
      metrics.batchFinished(batch, clock.instant())
    )

  /** Sent every few seconds while a query is running but has nothing new to read. */
  override def onQueryIdle(event: QueryIdleEvent): Unit =
    nameOf(event.id).foreach(query => metrics.queryAlive(query, clock.instant()))

  /** Nothing to count. The application ends when a query does and is started again. */
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
}
