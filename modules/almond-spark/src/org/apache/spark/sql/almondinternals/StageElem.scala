package org.apache.spark.sql.almondinternals

import almond.interpreter.api.OutputHandler

import java.util.concurrent.ThreadFactory
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt

final class StageElem(
  stageId: Int,
  numTasks: Int,
  keep: Boolean,
  name: String,
  details: String,
  useBars: Boolean
) {

  private val htmlName = StageElem.htmlName(name)

  val displayId      = s"stage-info-${Id.generate()}"
  val titleDisplayId = s"$displayId-title"

  val startedTasks = new AtomicInteger
  val doneTasks    = new AtomicInteger

  @volatile var stageDone0 = false

  def taskStart(): Unit = {
    startedTasks.incrementAndGet()
  }

  def taskDone(): Unit = {
    doneTasks.incrementAndGet()
  }

  def stageDone(): Unit = {
    stageDone0 = true
  }

  // Not used any more, kept for binary compatibility
  val extraStyle = Seq(
    "word-wrap: normal",
    "white-space: nowrap",
    "text-align: center"
  )

  def init(cancelStageCommTargetName: String, sendInitCode: Boolean)(implicit
    publish: OutputHandler
  ): Unit =
    if (useBars)
      publish.html(
        StageElem.barHtml(htmlName, numTasks, doneTasks = 0, startedTasks = 0),
        id = displayId
      )
    else
      publish.html(
        s"<div>$htmlName</div>",
        id = displayId
      )

  def update()(implicit publish: OutputHandler): Unit = {

    val doneTasks0    = doneTasks.get()
    val startedTasks0 = startedTasks.get()
    val onGoingCount  = startedTasks0 - doneTasks0

    val donePct = math.round(100.0 * doneTasks0.toDouble / numTasks).toInt

    if (useBars)
      publish.updateHtml(
        StageElem.barHtml(htmlName, numTasks, doneTasks0, startedTasks0),
        id = displayId
      )
    else {
      val taskOrTasks = if (numTasks <= 1) "task" else "tasks"
      val onGoingMessage =
        if (onGoingCount <= 0) ""
        else s", $onGoingCount on-going"
      val message =
        if (stageDone0) s"<div>$htmlName (done)</div>"
        else s"<div>$htmlName ($donePct% of $numTasks $taskOrTasks$onGoingMessage)</div>"
      publish.updateHtml(message, id = displayId)
    }

    if (stageDone0 && !keep) {
      // Allow the user to see the completed bar before wiping it
      val delay = 3.seconds
      val runnable: Runnable =
        () =>
          try {
            publish.updateHtml("", id = displayId)
            if (useBars)
              publish.updateHtml("", id = titleDisplayId)
          }
          catch {
            case t: Throwable =>
              System.err.println("Error while updating message")
              t.printStackTrace(System.err)
          }
      StageElem.scheduler.schedule(runnable, delay.length, delay.unit)
    }
  }

}

object StageElem {
  private def keepAlive = 30.seconds
  lazy val scheduler = {
    val executor = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactory {
        val count = new AtomicInteger
        override def newThread(r: Runnable): Thread = {
          val name = s"almond-spark-progress-${count.getAndIncrement()}"
          val t    = new Thread(r, name)
          t.setDaemon(true)
          t
        }
      }
    )
    executor.setKeepAliveTime(keepAlive.length, keepAlive.unit)
    executor
  }

  private def escape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  /** The stage name as HTML, like `<code>count</code> at <code>cmd3.sc:2</code>` */
  private[almondinternals] def htmlName(name: String): String = {
    val sep = " at "
    val idx = name.indexOf(sep)
    if (idx < 0) s"<code>${escape(name)}</code>"
    else
      s"<code>${escape(name.take(idx))}</code> at <code>${escape(name.drop(idx + sep.length))}</code>"
  }

  private def pct(count: Int, numTasks: Int): Int =
    if (numTasks <= 0) 0
    else math.max(0, math.min(100, math.round(100.0 * count.toDouble / numTasks).toInt))

  /** A progress bar, with the stage name and task counts, as HTML
    *
    * Styled inline, so that it renders the same in JupyterLab, Jupyter Notebook, or any other
    * front-end, without relying on a CSS framework. Colors come from the JupyterLab theme when
    * available (so that bars look right with its dark theme too), with fallbacks for other
    * front-ends.
    *
    * The bar shows the completed tasks, followed by the on-going ones in a lighter color.
    */
  private[almondinternals] def barHtml(
    htmlName: String,
    numTasks: Int,
    doneTasks: Int,
    startedTasks: Int
  ): String = {
    val donePct    = pct(doneTasks, numTasks)
    val onGoingPct = math.min(100 - donePct, pct(startedTasks - doneTasks, numTasks))
    val onGoing    = startedTasks - doneTasks
    val counts =
      if (onGoing <= 0) s"$doneTasks / $numTasks"
      else s"$doneTasks + $onGoing / $numTasks"
    val trackStyle = Seq(
      "flex: 1 1 auto",
      "min-width: 6em",
      "height: 0.8em",
      "display: flex",
      "overflow: hidden",
      "border-radius: 0.4em",
      "background-color: var(--jp-layout-color3, #e0e0e0)"
    ).mkString("; ")
    val doneStyle =
      s"width: $donePct%; background-color: var(--jp-brand-color1, #1976d2)"
    val onGoingStyle =
      s"width: $onGoingPct%; background-color: var(--jp-brand-color3, #90caf9)"
    s"""<div style="display: flex; align-items: center; gap: 0.75em; margin: 0.2em 0">
       |  <div style="flex: 0 1 auto; white-space: nowrap; overflow: hidden; text-overflow: ellipsis">$htmlName</div>
       |  <div role="progressbar" aria-valuenow="$donePct" aria-valuemin="0" aria-valuemax="100" style="$trackStyle">
       |    <div style="$doneStyle"></div>
       |    <div style="$onGoingStyle"></div>
       |  </div>
       |  <div style="flex: 0 0 auto; white-space: nowrap; font-variant-numeric: tabular-nums">$counts</div>
       |</div>""".stripMargin
  }

}
