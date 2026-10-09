package dev.wikiguard.common.config

/** Why a piece of configuration could not be used. */
sealed trait ConfigError {
  def message: String
}

object ConfigError {

  /** The file is missing or is not valid YAML. */
  final case class Unreadable(source: String, reason: String) extends ConfigError {
    def message: String = s"cannot read $source: $reason"
  }

  /** A required setting is absent or empty. */
  final case class Missing(key: String) extends ConfigError {
    def message: String = s"$key is not set"
  }

  /** A setting is present but its value makes no sense. */
  final case class Invalid(key: String, reason: String) extends ConfigError {
    def message: String = s"$key is invalid: $reason"
  }
}
