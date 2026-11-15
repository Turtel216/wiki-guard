package dev.wikiguard.stream.metrics

import scala.jdk.CollectionConverters._

import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.{CounterSnapshot, GaugeSnapshot}

/** Reads single values back out of a registry, the way a scrape would see them. */
object MetricValues {

  /**
   * The value of the one series with this name and these labels, if there is one. Counters are
   * looked up without the "_total" that a scrape adds to their name.
   */
  def value(registry: PrometheusRegistry, name: String, labels: (String, String)*): Option[Double] =
    registry
      .scrape()
      .asScala
      .filter(_.getMetadata.getName == name)
      .flatMap(_.getDataPoints.asScala)
      .filter(point =>
        labels.forall { case (label, wanted) => point.getLabels.get(label) == wanted }
      )
      .collectFirst {
        case counter: CounterSnapshot.CounterDataPointSnapshot => counter.getValue
        case gauge: GaugeSnapshot.GaugeDataPointSnapshot => gauge.getValue
      }
}
