package dev.wikiguard.batch.migrate

/** Why the lake tables could not be brought up to date. */
sealed trait MigrationError {
  def message: String
}

object MigrationError {

  /** The settings of the job are missing or wrong. */
  final case class BadConfig(reason: String) extends MigrationError {
    def message: String = reason
  }

  /** The directory or one of its files could not be read. */
  final case class Unreadable(path: String, reason: String) extends MigrationError {
    def message: String = s"cannot read $path: $reason"
  }

  /** There is nothing to apply, which is more likely a wrong path than an empty lake. */
  final case class NoScripts(directory: String) extends MigrationError {
    def message: String = s"$directory holds no .sql files"
  }

  /** A statement was rejected. Statements before it have been applied. */
  final case class StatementFailed(file: String, statement: String, reason: String)
      extends MigrationError {
    def message: String = s"$file failed at '${statement.takeWhile(_ != '\n')}': $reason"
  }
}
