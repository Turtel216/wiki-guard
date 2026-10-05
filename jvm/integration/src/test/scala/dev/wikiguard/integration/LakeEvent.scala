package dev.wikiguard.integration

import java.time.Instant

/** Row of the smoke test table. Field names match the column names. */
final case class LakeEvent(wiki: String, rev_id: Long, rev_ts: Instant, score: Double)
