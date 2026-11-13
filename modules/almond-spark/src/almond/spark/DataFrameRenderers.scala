package almond.spark

import org.apache.spark.sql.{Dataset, Row}

object DataFrameRenderer {

  // inspired by https://github.com/apache/incubator-toree/blob/5b19aac2e56a56d35c888acc4ed5e549b1f4ed7c/kernel/src/main/scala/org/apache/toree/utils/DataFrameConverter.scala#L47-L59
  def render(df: Dataset[Row], limit: Int = 10): String =
    // Dataset#take lets Spark optimize the limit, and we build the HTML on the driver
    // (so that scalatags isn't needed on the executors)
    renderRows(df.schema.fieldNames.toSeq, df.take(limit).toSeq)

  private[spark] def renderRows(fieldNames: Seq[String], dataRows: Seq[Row]): String = {
    import scalatags.Text.all._
    val header   = tr(fieldNames.map(th(_)))
    val rowsHtml = dataRows.map(row => tr(row.toSeq.map(fieldToString).map(td(_))))
    table(header, rowsHtml).render
  }

  // https://github.com/apache/incubator-toree/blob/5b19aac2e56a56d35c888acc4ed5e549b1f4ed7c/kernel/src/main/scala/org/apache/toree/utils/DataFrameConverter.scala#L84-L89
  def fieldToString(any: Any): String =
    any match {
      case null        => "null"
      case seq: Seq[_] => seq.mkString("[", ", ", "]")
      case _           => any.toString
    }

}
