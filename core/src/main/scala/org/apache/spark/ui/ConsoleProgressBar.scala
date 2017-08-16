/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui

import java.util.{Timer, TimerTask}

import org.apache.spark._
import org.apache.spark.internal.Logging

/**
 * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
 * status of active stages from `sc.statusTracker` periodically, the progress bar will be showed
 * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
 * of them will be combined together, showed in one line.
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {
  // Carriage return
  private val CR = '\r'
  // Update period of progress bar, in milliseconds
  private val updatePeriodMSec =
    sc.getConf.getTimeAsMs("spark.ui.consoleProgress.update.interval", "200")
  // Delay to show up a progress bar, in milliseconds
  private val firstDelayMSec = 500L

  // The width of terminal
  private val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
    sys.env.get("COLUMNS").get.toInt
  } else {
    80
  }

  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh progress", true)
  timer.schedule(new TimerTask{
    override def run() {
      refresh()
    }
  }, firstDelayMSec, updatePeriodMSec)

  /**
   * Try to refresh the progress bar in every cycle
   */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < firstDelayMSec) {
      return
    }
    val stageIds = sc.statusTracker.getActiveStageIds()
    val stages = stageIds.flatMap(sc.statusTracker.getStageInfo).filter(_.numTasks() > 1)
      .filter(now - _.submissionTime() > firstDelayMSec).sortBy(_.stageId())
    if (stages.length > 0) {
      show(now, stages.take(3))  // display at most 3 stages in same time
    }
  }

  /**
   * Show progress bar in console. The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def show(now: Long, stages: Seq[SparkStageInfo]) {
    val width = TerminalWidth / stages.size
    val bar = stages.map { s =>
      val total = s.numTasks()
      val header = s"[Stage ${s.stageId()}:"
      val tailer = s"(${s.numCompletedTasks()} + ${s.numActiveTasks()}) / $total]"
      val w = width - header.length - tailer.length
      val bar = if (w > 0) {
        val percent = w * s.numCompletedTasks() / total
        (0 until w).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
      } else {
        ""
      }
      header + bar + tailer
    }.mkString("")

    // only refresh if it's changed OR after 1 minute (or the ssh connection will be closed
    // after idle some time)
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      System.err.print(CR + bar)
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear() {
    if (!lastProgressBar.isEmpty) {
      System.err.printf(CR + " " * TerminalWidth + CR)
      lastProgressBar = ""
    }
  }

  /**
   * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
   * interweave with output of jobs.
   */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }

  /**
   * Tear down the timer thread.  The timer thread is a GC root, and it retains the entire
   * SparkContext if it's not terminated.
   */
  def stop(): Unit = timer.cancel()
}
