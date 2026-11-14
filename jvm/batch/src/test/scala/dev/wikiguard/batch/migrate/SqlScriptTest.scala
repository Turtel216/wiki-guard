package dev.wikiguard.batch.migrate

import org.scalatest.funsuite.AnyFunSuite

class SqlScriptTest extends AnyFunSuite {

  test("statements are split at semicolons and trimmed") {
    val text = "CREATE NAMESPACE a;\n\nCREATE TABLE a.b (\n  id BIGINT\n);\n"

    assert(
      SqlScript.statements(text) == Vector("CREATE NAMESPACE a", "CREATE TABLE a.b (\n  id BIGINT\n)")
    )
  }

  test("comment lines are left out, even with a semicolon in them") {
    val text = "-- first; second\nCREATE NAMESPACE a;\n  -- the end\n"

    assert(SqlScript.statements(text) == Vector("CREATE NAMESPACE a"))
  }

  test("a last statement without a semicolon still counts") {
    assert(SqlScript.statements("CREATE NAMESPACE a") == Vector("CREATE NAMESPACE a"))
  }

  test("a file with only comments and blank lines holds no statements") {
    assert(SqlScript.statements("-- nothing here\n\n   \n;\n").isEmpty)
  }
}
