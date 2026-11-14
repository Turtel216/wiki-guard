package dev.wikiguard.batch.migrate

/** Splits the text of a DDL file into the statements it holds. */
object SqlScript {

  /**
   * A semicolon ends a statement. Lines that hold nothing but a comment are left out, which means a
   * semicolon may appear in such a comment but nowhere else inside a statement.
   */
  def statements(text: String): Vector[String] =
    text.linesIterator
      .filterNot(line => line.trim.startsWith("--"))
      .mkString("\n")
      .split(';')
      .toVector
      .map(_.trim)
      .filter(_.nonEmpty)
}
