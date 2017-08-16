/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.executor

import org.apache.spark.{TaskCommitDenied, TaskFailedReason}

/**
 * Exception thrown when a task attempts to commit output to HDFS but is denied by the driver.
 */
private[spark] class CommitDeniedException(
    msg: String,
    jobID: Int,
    splitID: Int,
    attemptNumber: Int)
  extends Exception(msg) {

  def toTaskFailedReason: TaskFailedReason = TaskCommitDenied(jobID, splitID, attemptNumber)
}
