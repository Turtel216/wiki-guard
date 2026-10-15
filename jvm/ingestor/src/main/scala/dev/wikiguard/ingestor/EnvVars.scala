package dev.wikiguard.ingestor

import dev.wikiguard.common.config.ConfigError

/** Reads settings from environment variables. A blank value counts as not set. */
private[ingestor] object EnvVars {

  def value(env: Map[String, String], key: String): Option[String] =
    env.get(key).map(_.trim).filter(_.nonEmpty)

  def required(env: Map[String, String], key: String): Either[ConfigError, String] =
    value(env, key).toRight(ConfigError.Missing(key))
}
