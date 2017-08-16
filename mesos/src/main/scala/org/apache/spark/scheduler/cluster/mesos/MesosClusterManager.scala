/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * Cluster Manager for creation of Yarn scheduler and backend
 */
private[spark] class MesosClusterManager extends ExternalClusterManager {
  private val MESOS_REGEX = """mesos://(.*)""".r

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("mesos")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
      "I/O encryption is currently not supported in Mesos.")

    val mesosUrl = MESOS_REGEX.findFirstMatchIn(masterURL).get.group(1)
    val coarse = sc.conf.getBoolean("spark.mesos.coarse", defaultValue = true)
    if (coarse) {
      new MesosCoarseGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl,
        sc.env.securityManager)
    } else {
      new MesosFineGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl)
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}

