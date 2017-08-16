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

import scala.collection.mutable.HashMap

/**
 * Small helper for tracking failed tasks for blacklisting purposes.  Info on all failures on one
 * executor, within one task set.
 */
private[scheduler] class ExecutorFailuresInTaskSet(val node: String) {
  /**
   * Mapping from index of the tasks in the taskset, to the number of times it has failed on this
   * executor.
   */
  val taskToFailureCount = HashMap[Int, Int]()

  def updateWithFailure(taskIndex: Int): Unit = {
    val prevFailureCount = taskToFailureCount.getOrElse(taskIndex, 0)
    taskToFailureCount(taskIndex) = prevFailureCount + 1
  }

  def numUniqueTasksWithFailures: Int = taskToFailureCount.size

  /**
   * Return the number of times this executor has failed on the given task index.
   */
  def getNumTaskFailures(index: Int): Int = {
    taskToFailureCount.getOrElse(index, 0)
  }

  override def toString(): String = {
    s"numUniqueTasksWithFailures = $numUniqueTasksWithFailures; " +
      s"tasksToFailureCount = $taskToFailureCount"
  }
}
