/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicBoolean

private[spark] class SimpleExtensionService extends SchedulerExtensionService {

  /** started flag; set in the `start()` call, stopped in `stop()`. */
  val started = new AtomicBoolean(false)

  override def start(binding: SchedulerExtensionServiceBinding): Unit = {
    started.set(true)
  }

  override def stop(): Unit = {
    started.set(false)
  }
}
