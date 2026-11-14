package dev.wikiguard.batch.migrate

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import dev.wikiguard.common.config.EnvVars
import dev.wikiguard.common.lake.LakeCatalog

/**
 * Creates the lake tables and applies changes to them.
 *
 * It runs every .sql file of a directory in the order of the file names. Nothing remembers which
 * files have run before, so every statement must be safe to run again: CREATE ... IF NOT EXISTS,
 * ADD COLUMN IF NOT EXISTS and the like.
 */
object MigrateLake {

  val DirectoryKey: String = "WG_DDL_DIR"

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val result = for {
      catalog <- LakeCatalog.fromEnv(sys.env).left.map(e => MigrationError.BadConfig(e.message))
      directory <- EnvVars
        .required(sys.env, DirectoryKey)
        .left
        .map(e => MigrationError.BadConfig(e.message))
      spark = SparkSession.builder().appName("wikiguard-migrate-lake").getOrCreate()
      applied <- run(spark, catalog, Paths.get(directory))
    } yield applied
    result match {
      case Right(count) => logger.info("Applied {} statements", count)
      case Left(error) => throw new IllegalStateException(s"Migration failed: ${error.message}")
    }
  }

  /** Applies every script and returns how many statements ran. Stops at the first failure. */
  def run(spark: SparkSession, catalog: String, directory: Path): Either[MigrationError, Int] =
    for {
      scripts <- scripts(directory)
      _ <- execute(spark, directory.toString, s"USE $catalog")
      counts <- traverse(scripts)(script => apply(spark, script))
    } yield counts.sum

  private def scripts(directory: Path): Either[MigrationError, Vector[Path]] =
    Try {
      Using.resource(Files.list(directory)) { files =>
        files.iterator.asScala.toVector.filter(_.toString.endsWith(".sql")).sortBy(_.getFileName)
      }
    }.toEither.left
      .map(error => MigrationError.Unreadable(directory.toString, error.toString))
      .filterOrElse(_.nonEmpty, MigrationError.NoScripts(directory.toString))

  private def apply(spark: SparkSession, script: Path): Either[MigrationError, Int] = {
    val name = script.getFileName.toString
    for {
      text <- Try(Files.readString(script)).toEither.left
        .map(error => MigrationError.Unreadable(script.toString, error.toString))
      statements = SqlScript.statements(text)
      _ <- traverse(statements)(statement => execute(spark, name, statement))
    } yield {
      logger.info("Applied {} ({} statements)", name, statements.size)
      statements.size
    }
  }

  private def execute(
      spark: SparkSession,
      file: String,
      statement: String
  ): Either[MigrationError, Unit] =
    Try(spark.sql(statement)).toEither
      .map(_ => ())
      .left
      .map(error => MigrationError.StatementFailed(file, statement, error.toString))

  /** Runs the steps in order and stops at the first one that fails. */
  private def traverse[A, B](items: Vector[A])(
      step: A => Either[MigrationError, B]
  ): Either[MigrationError, Vector[B]] =
    items.foldLeft[Either[MigrationError, Vector[B]]](Right(Vector.empty)) { (done, item) =>
      done.flatMap(results => step(item).map(results :+ _))
    }
}
