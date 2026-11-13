package almond.spark

object ToreeSql {

  var sqlLimit = 10

  /** Turns `s` into a Scala string literal */
  private[spark] def stringLiteral(s: String): String = {
    val escaped = s.flatMap {
      case '"'          => "\\\""
      case '\\'         => "\\\\"
      case '\n'         => "\\n"
      case '\r'         => "\\r"
      case '\t'         => "\\t"
      case c if c < ' ' => "\\u%04x".format(c.toInt)
      case c            => c.toString
    }
    "\"" + escaped + "\""
  }

  private[spark] def sqlMagic(content: String): String = {
    val query = stringLiteral(content)
    s"_root_.almond.display.Html(_root_.almond.spark.DataFrameRenderer.render(spark.sql($query), limit = $sqlLimit))"
  }

  def setup(): Unit = {
    almond.toree.CellMagicHook.addHandler("sql") { (_, content) =>
      Right(sqlMagic(content))
    }
    almond.toree.CellMagicHook.addHandler("sparksql") { (_, content) =>
      Right(sqlMagic(content))
    }
  }
}
