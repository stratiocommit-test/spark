/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.master

import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}

private[master] class ExecutorDesc(
    val id: Int,
    val application: ApplicationInfo,
    val worker: WorkerInfo,
    val cores: Int,
    val memory: Int) {

  var state = ExecutorState.LAUNCHING

  /** Copy all state (non-val) variables from the given on-the-wire ExecutorDescription. */
  def copyState(execDesc: ExecutorDescription) {
    state = execDesc.state
  }

  def fullId: String = application.id + "/" + id

  override def equals(other: Any): Boolean = {
    other match {
      case info: ExecutorDesc =>
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode: Int = toString.hashCode()
}
