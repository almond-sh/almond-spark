package almond.spark

import utest._

object ToreeSqlTests extends TestSuite {
  val tests = Tests {

    test("string literal") {
      assert(ToreeSql.stringLiteral("select 1") == "\"select 1\"")
      assert(
        ToreeSql.stringLiteral("a\"\"\"b\\c\nd\te") ==
          "\"a\\\"\\\"\\\"b\\\\c\\nd\\te\""
      )
      assert(ToreeSql.stringLiteral("\u0001") == "\"\\u0001\"")
    }

    test("sql magic") {
      val code = ToreeSql.sqlMagic("select \"\"\" from t")
      assert(code.contains("spark.sql(\"select \\\"\\\"\\\" from t\")"))
    }
  }
}
