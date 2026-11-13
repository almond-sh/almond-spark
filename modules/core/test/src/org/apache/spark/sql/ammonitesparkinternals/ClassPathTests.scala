package org.apache.spark.sql.ammonitesparkinternals

import utest._

object ClassPathTests extends TestSuite {
  val tests = Tests {

    test("expand class path entries") {
      val dir = os.temp.dir(prefix = "ammonite-spark-tests")
      try {
        os.write(dir / "a.jar", "")
        os.write(dir / "b.JAR", "")
        os.write(dir / "c.txt", "")
        os.makeDir(dir / "sub")

        val jars = Set(dir / "a.jar", dir / "b.JAR")

        val viaStar = AmmoniteSparkSessionBuilder.expandClassPathEntry(s"$dir/*")
        assert(viaStar.toSet == jars)

        val viaStarJar = AmmoniteSparkSessionBuilder.expandClassPathEntry(s"$dir/*.jar")
        assert(viaStarJar.toSet == jars)

        val single = AmmoniteSparkSessionBuilder.expandClassPathEntry(s"$dir/c.txt")
        assert(single == Seq(dir / "c.txt"))

        val missing = AmmoniteSparkSessionBuilder.expandClassPathEntry(s"$dir/nope/*.jar")
        assert(missing.isEmpty)
      }
      finally os.remove.all(dir)
    }
  }
}
