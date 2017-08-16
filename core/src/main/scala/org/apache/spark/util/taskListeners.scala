/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import java.util.EventListener

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when a task's execution completes.
 */
@DeveloperApi
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext): Unit
}


/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when a task's execution encounters an error.
 * Operations defined here must be idempotent, as `onTaskFailure` can be called multiple times.
 */
@DeveloperApi
trait TaskFailureListener extends EventListener {
  def onTaskFailure(context: TaskContext, error: Throwable): Unit
}


/**
 * Exception thrown when there is an exception in executing the callback in TaskCompletionListener.
 */
private[spark]
class TaskCompletionListenerException(
    errorMessages: Seq[String],
    val previousError: Option[Throwable] = None)
  extends RuntimeException {

  override def getMessage: String = {
    if (errorMessages.size == 1) {
      errorMessages.head
    } else {
      errorMessages.zipWithIndex.map { case (msg, i) => s"Exception $i: $msg" }.mkString("\n")
    } +
    previousError.map { e =>
      "\n\nPrevious exception in task: " + e.getMessage + "\n" +
        e.getStackTrace.mkString("\t", "\n\t", "")
    }.getOrElse("")
  }
}
