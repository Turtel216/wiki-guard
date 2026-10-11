package dev.wikiguard.ingestor

import scala.concurrent.duration.{Duration, FiniteDuration}

/** How long to wait before reconnecting after failures in a row. */
final case class Backoff(min: FiniteDuration, max: FiniteDuration) {

  /**
   * The wait doubles with every failure until it reaches the maximum.
   *
   * @param failures
   *   how many attempts in a row have failed; zero means no wait
   * @param jitter
   *   a random number from 0 to 1 that shortens the wait by up to half, so that clients that failed
   *   together do not all come back together
   */
  def delay(failures: Int, jitter: Double): FiniteDuration =
    if (failures <= 0) {
      Duration.Zero
    } else {
      val doublings = math.min(failures - 1, Backoff.MaxDoublings)
      val base = (min * (1L << doublings).toDouble).min(max)
      val shortened = base * (0.5 + 0.5 * jitter.max(0.0).min(1.0))
      Backoff.finite(shortened).max(min)
    }
}

object Backoff {

  // Far more than any sensible maximum needs, and small enough not to overflow.
  private val MaxDoublings = 20

  private def finite(duration: Duration): FiniteDuration =
    Duration.fromNanos(duration.toNanos)
}
