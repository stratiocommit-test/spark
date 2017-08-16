/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import java.util.Date

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}

class AllStagesResourceSuite extends SparkFunSuite {

  def getFirstTaskLaunchTime(taskLaunchTimes: Seq[Long]): Option[Date] = {
    val tasks = new LinkedHashMap[Long, TaskUIData]
    taskLaunchTimes.zipWithIndex.foreach { case (time, idx) =>
      tasks(idx.toLong) = TaskUIData(
        new TaskInfo(idx, idx, 1, time, "", "", TaskLocality.ANY, false), None)
    }

    val stageUiData = new StageUIData()
    stageUiData.taskData = tasks
    val status = StageStatus.ACTIVE
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc")
    val stageData = AllStagesResource.stageUiToStageData(status, stageInfo, stageUiData, false)

    stageData.firstTaskLaunchedTime
  }

  test("firstTaskLaunchedTime when there are no tasks") {
    val result = getFirstTaskLaunchTime(Seq())
    assert(result == None)
  }

  test("firstTaskLaunchedTime when there are tasks but none launched") {
    val result = getFirstTaskLaunchTime(Seq(-100L, -200L, -300L))
    assert(result == None)
  }

  test("firstTaskLaunchedTime when there are tasks and some launched") {
    val result = getFirstTaskLaunchTime(Seq(-100L, 1449255596000L, 1449255597000L))
    assert(result == Some(new Date(1449255596000L)))
  }

}
