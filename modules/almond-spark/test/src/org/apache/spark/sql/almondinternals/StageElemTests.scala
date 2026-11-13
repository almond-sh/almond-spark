package org.apache.spark.sql.almondinternals

import utest._

object StageElemTests extends TestSuite {
  val tests = Tests {

    test("html name") {
      assert(StageElem.htmlName("sum at cmd3.sc:2") == "<code>sum</code> at <code>cmd3.sc:2</code>")
      assert(StageElem.htmlName("a <b>") == "<code>a &lt;b&gt;</code>")
    }

    test("bar") {
      val html =
        StageElem.barHtml("<code>sum</code>", numTasks = 40, doneTasks = 10, startedTasks = 14)
      assert(html.contains("<code>sum</code>"))
      assert(html.contains("width: 25%;"))
      assert(html.contains("width: 10%;"))
      assert(html.contains("10 + 4 / 40"))
      assert(html.contains("aria-valuenow=\"25\""))
      // styled inline, rather than via CSS classes the front-end may not have
      assert(!html.contains("class="))
    }

    test("done bar") {
      val html =
        StageElem.barHtml("<code>sum</code>", numTasks = 40, doneTasks = 40, startedTasks = 40)
      assert(html.contains("width: 100%;"))
      assert(html.contains("width: 0%;"))
      assert(html.contains(">40 / 40<"))
    }

    test("no tasks") {
      val html =
        StageElem.barHtml("<code>sum</code>", numTasks = 0, doneTasks = 0, startedTasks = 0)
      assert(html.contains("width: 0%;"))
      assert(html.contains("0 / 0"))
    }

  }
}
