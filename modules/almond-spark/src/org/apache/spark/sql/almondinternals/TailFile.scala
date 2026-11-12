package org.apache.spark.sql.almondinternals

import java.io.{BufferedReader, File, FileReader}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

private[almondinternals] object TailFile {

  /** Reads `f` like `tail -f` would, passing new lines to `onLines` by batches
    *
    * Returns once `keepReading` returns false. Throws [[InterruptedException]] if the current
    * thread is interrupted while waiting for new content.
    */
  // https://stackoverflow.com/questions/557844/java-io-implementation-of-unix-linux-tail-f/558262#558262
  def apply(
    f: File,
    keepReading: () => Boolean,
    delay: FiniteDuration,
    lineBufferSize: Int
  )(
    onLines: List[String] => Unit
  ): Unit = {
    val r = new FileReader(f)
    try {
      val br    = new BufferedReader(r)
      val lines = new ListBuffer[String]
      while (keepReading()) {
        val line = br.readLine()
        if (line == null)
          // wait until there is more in the file
          Thread.sleep(delay.toMillis)
        else {
          lines += line

          while (keepReading() && lines.length <= lineBufferSize && br.ready())
            for (line <- Option(br.readLine()))
              lines += line

          val l = lines.toList
          lines.clear()
          onLines(l)
        }
      }
    }
    finally r.close()
  }

}
