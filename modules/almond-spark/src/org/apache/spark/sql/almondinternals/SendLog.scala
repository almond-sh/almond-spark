package org.apache.spark.sql.almondinternals

import java.io.File
import java.nio.file.{Files, StandardOpenOption}

import almond.interpreter.api.{CommHandler, CommTarget, DisplayData, OutputHandler}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets

final class SendLog(
  f: File,
  commHandler: CommHandler,
  threadName: String = "send-log",
  prefix: Option[String] = None,
  fileName: String = null,
  lineBufferSize: Int = 10,
  delay: FiniteDuration = 2.seconds,
  initialBackOffDelay: FiniteDuration = 5.seconds,
  maxBackOffDelay: FiniteDuration = 1.minute,
  backOffFactor: Double = 2.0,
  replaceHome: Boolean = true
) {

  assert(backOffFactor >= 1.0)

  private val commTarget = Id.generate()
  private val commId     = Id.generate()

  @volatile private var gotAck      = false
  @volatile private var keepReading = true

  commHandler.registerCommTarget(
    commTarget + "-ack",
    CommTarget { (_, _) =>
      gotAck = true
    }
  )

  private def withExponentialBackOff[T](f: => T): T = {

    def helper(delay: Duration): T = {
      val res =
        try Some(f)
        catch {
          case NonFatal(e) =>
            System.err.println(
              s"Warning: error while sending logs ($e), retrying in ${delay.toSeconds} s"
            )
            None
        }

      res match {
        case Some(t) => t
        case None =>
          assert(delay.isFinite)
          Thread.sleep(delay.toMillis)
          helper((backOffFactor * delay).min(maxBackOffDelay))
      }
    }

    helper(initialBackOffDelay)
  }

  val thread: Thread =
    new Thread(threadName) {
      override def run(): Unit = {

        import com.github.plokhotnyuk.jsoniter_scala.core.writeToArray

        val fileName0 = Option(fileName).getOrElse {
          val p    = f.getAbsolutePath
          val home = sys.props("user.home")
          if (replaceHome && p.startsWith(home))
            "~" + p.stripPrefix(home)
          else
            p
        }

        val msg = writeToArray(SendLog.Open(fileName0, prefix))

        try {
          withExponentialBackOff {
            commHandler.commOpen(
              targetName = commTarget,
              id = commId,
              data = msg,
              metadata = SendLog.emptyObj
            )
          }

          while (!gotAck)
            Thread.sleep(delay.toMillis)

          TailFile(f, () => keepReading, delay, lineBufferSize) { lines =>
            val lines0 =
              if (replaceHome) {
                val home = sys.props("user.home")
                lines.map(_.replace(home, "~"))
              }
              else
                lines
            val res = writeToArray(SendLog.Data(lines0))
            withExponentialBackOff {
              commHandler.commMessage(commId, res, SendLog.emptyObj)
            }
          }
        }
        catch {
          case _: InterruptedException =>
          // normal exit
        }
        finally
          // no re-attempt here…
          commHandler.commClose(commId, SendLog.emptyObj, SendLog.emptyObj)
      }
    }

  def init()(implicit outputHandler: OutputHandler): Unit =
    outputHandler.js(SendLog.jsInit(commTarget))

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

object SendLog {

  private final case class Open(file_name: String, prefix: Option[String])

  private object Open {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[Open] =
      JsonCodecMaker.make
  }

  private final case class Data(data: Seq[String])

  private object Data {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[Data] =
      JsonCodecMaker.make
  }

  private val emptyObj = "{}".getBytes(StandardCharsets.UTF_8)

  def start(
    f: File,
    prefix: String = null
  )(implicit
    commHandler: CommHandler,
    outputHandler: OutputHandler
  ): SendLog = {

    lazy val sendLog = new SendLog(f, commHandler, prefix = Option(prefix))

    val id = Id.generate()
    val data = DisplayData(
      Map(DisplayData.ContentType.text -> "", DisplayData.ContentType.html -> ""),
      idOpt = Some(id)
    )
    outputHandler.display(data)
    val commName = s"spark-logs-$id"
    commHandler.receiver(
      commName,
      onOpen = (_, _) => {
        val msg = "See your browser developer console for detailed spark logs."
        val updatedData = DisplayData(
          Map(
            DisplayData.ContentType.text -> msg,
            DisplayData.ContentType.html -> s"<p>$msg</p>"
          ),
          idOpt = Some(id)
        )
        outputHandler.updateDisplay(updatedData)
        sendLog.start()
      }
    )(_ => ())

    // It seems the file must exist for the reader above to get content appended to it.
    if (!f.exists())
      Files.write(f.toPath, Array.emptyByteArray, StandardOpenOption.CREATE)

    sendLog.init()
    sendLog
  }

  private def jsInit(target: String): String =
    s"""
      if (typeof Jupyter != "undefined") {
        var startingComm = Jupyter.notebook.kernel.comm_manager.new_comm("$target-starting", "{}");
        startingComm.open();
        startingComm.send({});

        Jupyter.notebook.kernel.comm_manager.register_target(
          "$target",
          function (comm, data) {
            console.log("$$ tail -F " + data.content.data.file_name);
            var prefix = data.content.data.prefix || "";

            var ackComm = Jupyter.notebook.kernel.comm_manager.new_comm("$target-ack", "{}");
            ackComm.open();
            ackComm.send({});

            comm.on_msg(
              function (msg) {
                var a = msg.content.data.data;
                var len = a.length;
                for (var i = 0; i < len; i++) {
                  console.log(prefix + a[i]);
                }
              }
            );
          }
        );
      }
    """

}
