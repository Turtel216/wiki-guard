package dev.wikiguard.common.config

import scala.concurrent.duration.FiniteDuration

/** How to connect to the Wikimedia event stream. */
final case class StreamSettings(
    url: String,
    names: Vector[String],
    reconnectMin: FiniteDuration,
    reconnectMax: FiniteDuration,
    idleTimeout: FiniteDuration,
    upstreamRetention: FiniteDuration
) {

  /** All stream names are requested over one connection. */
  def connectUrl: String = s"$url/${names.mkString(",")}"
}
