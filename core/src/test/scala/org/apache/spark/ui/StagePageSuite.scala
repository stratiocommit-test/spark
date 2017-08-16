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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.{JobProgressListener, StagePage, StagesTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener

class StagePageSuite extends SparkFunSuite with LocalSparkContext {

  private val peakExecutionMemory = 10

  test("peak execution memory should displayed") {
    val conf = new SparkConf(false)
    val html = renderStagePage(conf).toString().toLowerCase
    val targetString = "peak execution memory"
    assert(html.contains(targetString))
  }

  test("SPARK-10543: peak execution memory should be per-task rather than cumulative") {
    val conf = new SparkConf(false)
    val html = renderStagePage(conf).toString().toLowerCase
    // verify min/25/50/75/max show task value not cumulative values
    assert(html.contains(s"<td>$peakExecutionMemory.0 b</td>" * 5))
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy stage to populate the page with useful content.
   */
  private def renderStagePage(conf: SparkConf): Seq[Node] = {
    val jobListener = new JobProgressListener(conf)
    val graphListener = new RDDOperationGraphListener(conf)
    val executorsListener = new ExecutorsListener(new StorageStatusListener(conf), conf)
    val tab = mock(classOf[StagesTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(conf)
    when(tab.progressListener).thenReturn(jobListener)
    when(tab.operationGraphListener).thenReturn(graphListener)
    when(tab.executorsListener).thenReturn(executorsListener)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(request.getParameter("id")).thenReturn("0")
    when(request.getParameter("attempt")).thenReturn("0")
    val page = new StagePage(tab)

    // Simulate a stage in job progress listener
    val stageInfo = new StageInfo(0, 0, "dummy", 1, Seq.empty, Seq.empty, "details")
    // Simulate two tasks to test PEAK_EXECUTION_MEMORY correctness
    (1 to 2).foreach {
      taskId =>
        val taskInfo = new TaskInfo(taskId, taskId, 0, 0, "0", "localhost", TaskLocality.ANY, false)
        jobListener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))
        jobListener.onTaskStart(SparkListenerTaskStart(0, 0, taskInfo))
        taskInfo.markFinished(TaskState.FINISHED)
        val taskMetrics = TaskMetrics.empty
        taskMetrics.incPeakExecutionMemory(peakExecutionMemory)
        jobListener.onTaskEnd(
          SparkListenerTaskEnd(0, 0, "result", Success, taskInfo, taskMetrics))
    }
    jobListener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
    page.render(request)
  }

}
