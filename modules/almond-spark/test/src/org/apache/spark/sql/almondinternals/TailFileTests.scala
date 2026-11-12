package org.apache.spark.sql.almondinternals

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import utest._

object TailFileTests extends TestSuite {
  val tests = Tests {

    test("read existing and appended lines") {
      val f = Files.createTempFile("almond-spark-tail", ".log")
      try {
        Files.write(f, "a\nb\n".getBytes(StandardCharsets.UTF_8))

        val received     = new ConcurrentLinkedQueue[String]
        val gotAll       = new CountDownLatch(1)
        @volatile var go = true

        val thread = new Thread {
          override def run(): Unit =
            TailFile(f.toFile, () => go, 10.millis, lineBufferSize = 1) { lines =>
              received.addAll(lines.asJava)
              if (received.size >= 4)
                gotAll.countDown()
            }
        }
        thread.start()

        Files.write(
          f,
          "c\nd\n".getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.APPEND
        )

        assert(gotAll.await(10L, TimeUnit.SECONDS))
        go = false
        thread.join(10000L)
        assert(!thread.isAlive)
        assert(received.asScala.toList == List("a", "b", "c", "d"))
      }
      finally Files.deleteIfExists(f)
    }
  }
}
