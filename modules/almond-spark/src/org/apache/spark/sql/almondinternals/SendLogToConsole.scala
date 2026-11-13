package org.apache.spark.sql.almondinternals

import almond.api.JupyterApi

import java.io.{File, PrintStream}
import java.nio.file.{Files, StandardOpenOption}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class SendLogToConsole(
  f: File,
  out: PrintStream,
  threadName: String = "send-log-console",
  lineBufferSize: Int = 10,
  delay: FiniteDuration = 2.seconds
) {

  @volatile private var keepReading = true

  val thread: Thread =
    new Thread(threadName) {
      override def run(): Unit = {

        try
          TailFile(f, () => keepReading, delay, lineBufferSize) { lines =>
            lines.foreach(out.println)
          }
        catch {
          case _: InterruptedException =>
          // normal exit
          case e: Throwable =>
            System.err.println(s"Thread $threadName crashed")
            e.printStackTrace(System.err)
        }
      }
    }

  def start(): Unit = {
    assert(keepReading, "Already stopped")
    if (!thread.isAlive)
      synchronized {
        if (!thread.isAlive)
          thread.start()
      }
  }

  def stop(): Unit = {
    keepReading = false
    thread.interrupt()
  }

}

object SendLogToConsole {

  def start(f: File)(implicit jupyterApi: JupyterApi): SendLogToConsole = {

    // It seems the file must exist for the reader above to get content appended to it.
    if (!f.exists())
      Files.write(f.toPath, Array.emptyByteArray, StandardOpenOption.CREATE)

    val sendLog = new SendLogToConsole(f, jupyterApi.consoleErr)
    sendLog.start()
    sendLog
  }

}
