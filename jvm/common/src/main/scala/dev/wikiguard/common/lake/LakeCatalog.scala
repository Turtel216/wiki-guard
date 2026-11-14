package dev.wikiguard.common.lake

import dev.wikiguard.common.config.{ConfigError, EnvVars}

/** The name under which Spark knows the Iceberg catalog of the lake. */
object LakeCatalog {

  val Key: String = "WG_LAKE_CATALOG"

  val Default: String = "lake"

  /** The name ends up inside SQL, so only a plain identifier is accepted. */
  def fromEnv(env: Map[String, String]): Either[ConfigError, String] =
    EnvVars.value(env, Key).fold[Either[ConfigError, String]](Right(Default)) { name =>
      Either.cond(
        name.matches("[a-z_][a-z0-9_]*"),
        name,
        ConfigError.Invalid(Key, "must be lower case letters, digits and underscores")
      )
    }
}
