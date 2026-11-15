package dev.wikiguard.stream.metrics

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import org.apache.spark.sql.streaming.StreamingQueryProgress

/**
 * What one finished batch of a streaming query says about the health of that query.
 *
 * @param duration
 *   how long the whole batch took, from looking for new data to committing the result
 * @param lagByTopic
 *   records still waiting in each Kafka topic when the batch stopped reading
 */
final case class BatchProgress(
    query: String,
    inputRows: Long,
    duration: FiniteDuration,
    lagByTopic: Map[String, Long]
)

object BatchProgress {

  /** The key under which Spark reports the length of the whole batch. */
  private val WholeBatch = "triggerExecution"

  /** A query without a name cannot be told apart from others, so it is not reported. */
  def from(progress: StreamingQueryProgress): Option[BatchProgress] =
    Option(progress.name).map { query =>
      val millis = Option(progress.durationMs.get(WholeBatch)).fold(0L)(_.longValue)
      val lags = progress.sources.toVector
        .map(source => KafkaLag.byTopic(source.endOffset, source.latestOffset))
      BatchProgress(query, progress.numInputRows, millis.millis, sumByTopic(lags))
    }

  /** A query can read the same topic through more than one source. */
  private def sumByTopic(lags: Vector[Map[String, Long]]): Map[String, Long] =
    lags.flatten.groupMapReduce { case (topic, _) => topic } { case (_, lag) => lag }(_ + _)
}
