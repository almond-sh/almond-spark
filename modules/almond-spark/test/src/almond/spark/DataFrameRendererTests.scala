package almond.spark

import org.apache.spark.sql.Row
import utest._

object DataFrameRendererTests extends TestSuite {
  val tests = Tests {

    test("fieldToString") {
      assert(DataFrameRenderer.fieldToString(null) == "null")
      assert(DataFrameRenderer.fieldToString(Seq(1, 2)) == "[1, 2]")
      assert(DataFrameRenderer.fieldToString(2) == "2")
    }

    test("renderRows") {
      val html = DataFrameRenderer.renderRows(
        Seq("a", "b"),
        Seq(Row(1, "<x>"), Row(null, Seq("u", "v")))
      )
      val expected =
        "<table><tr><th>a</th><th>b</th></tr>" +
          "<tr><td>1</td><td>&lt;x&gt;</td></tr>" +
          "<tr><td>null</td><td>[u, v]</td></tr></table>"
      assert(html == expected)
    }
  }
}
