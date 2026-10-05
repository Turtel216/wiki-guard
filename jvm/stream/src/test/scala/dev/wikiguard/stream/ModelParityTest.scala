package dev.wikiguard.stream

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Checks that XGBoost4J scores a model trained in Python exactly like Python does.
 *
 * The fixture is written by the Python package (python -m wikiguard_ml.parity) into the directory
 * named by WG_PARITY_DIR. It holds the model, the rows and the scores Python got.
 */
class ModelParityTest extends AnyFunSuite {

  private val Tolerance = 1e-6f

  test("XGBoost4J agrees with Python on a Python-trained model") {
    val dir = fixtureDir()
    val rows = readRows(dir.resolve("rows.csv"))
    val expected = readFloats(dir.resolve("scores.csv"))
    assert(rows.nonEmpty, "the fixture has no rows")
    assert(rows.length == expected.length, "the fixture needs one score per row")

    val actual = score(dir.resolve("model.ubj"), rows)

    val worst = actual.zip(expected).map { case (a, e) => math.abs(a - e) }.max
    assert(worst <= Tolerance, s"largest score difference was $worst")
  }

  /** A missing fixture cancels the test locally but fails it in CI, where it must exist. */
  private def fixtureDir(): Path = {
    val dir = sys.env.get("WG_PARITY_DIR").map(Paths.get(_)).filter(Files.isDirectory(_))
    dir.getOrElse {
      val message = "WG_PARITY_DIR does not point to a parity fixture; run `make test`"
      if (sys.env.contains("CI")) fail(message) else cancel(message)
    }
  }

  private def score(model: Path, rows: Vector[Vector[Float]]): Vector[Float] = {
    val booster = XGBoost.loadModel(model.toString)
    val columns = rows.headOption.fold(0)(_.length)
    val matrix = new DMatrix(rows.flatten.toArray, rows.length, columns, Float.NaN)
    try booster.predict(matrix).toVector.flatMap(_.headOption)
    finally matrix.delete()
  }

  private def readRows(file: Path): Vector[Vector[Float]] =
    readLines(file).map(line => line.split(',').toVector.map(_.toFloat))

  private def readFloats(file: Path): Vector[Float] =
    readLines(file).map(_.toFloat)

  private def readLines(file: Path): Vector[String] =
    Files.readAllLines(file).asScala.toVector.filter(_.nonEmpty)
}
