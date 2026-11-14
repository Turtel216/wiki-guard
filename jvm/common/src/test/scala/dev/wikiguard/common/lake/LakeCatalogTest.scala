package dev.wikiguard.common.lake

import org.scalatest.funsuite.AnyFunSuite

class LakeCatalogTest extends AnyFunSuite {

  test("the catalog has a default name") {
    assert(LakeCatalog.fromEnv(Map.empty) == Right("lake"))
  }

  test("a name from the environment replaces the default") {
    assert(LakeCatalog.fromEnv(Map(LakeCatalog.Key -> "lake_dev")) == Right("lake_dev"))
  }

  test("a name that is not a plain identifier is rejected") {
    val result = LakeCatalog.fromEnv(Map(LakeCatalog.Key -> "lake; DROP TABLE x"))

    assert(result.left.exists(_.message.contains(LakeCatalog.Key)))
  }
}
