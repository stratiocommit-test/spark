/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

import org.apache.spark.TaskContext

class FakeTask(
    stageId: Int,
    partitionId: Int,
    prefLocs: Seq[TaskLocation] = Nil) extends Task[Int](stageId, 0, partitionId) {
  override def runTask(context: TaskContext): Int = 0
  override def preferredLocations: Seq[TaskLocation] = prefLocs
}

object FakeTask {
  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred
   * locations for each task (given as varargs) if this sequence is not empty.
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageAttemptId = 0, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageId = 0, stageAttemptId, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageId: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*):
  TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = 0, null)
  }
}
