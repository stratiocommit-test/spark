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

import java.util.Date

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.util.Utils

private[deploy] class DriverInfo(
    val startTime: Long,
    val id: String,
    val desc: DriverDescription,
    val submitDate: Date)
  extends Serializable {

  @transient var state: DriverState.Value = DriverState.SUBMITTED
  /* If we fail when launching the driver, the exception is stored here. */
  @transient var exception: Option[Exception] = None
  /* Most recent worker assigned to this driver */
  @transient var worker: Option[WorkerInfo] = None

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init(): Unit = {
    state = DriverState.SUBMITTED
    worker = None
    exception = None
  }
}
