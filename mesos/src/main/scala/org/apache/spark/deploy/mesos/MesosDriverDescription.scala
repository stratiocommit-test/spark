/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.mesos

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.deploy.Command
import org.apache.spark.scheduler.cluster.mesos.MesosClusterRetryState

/**
 * Describes a Spark driver that is submitted from the
 * [[org.apache.spark.deploy.rest.mesos.MesosRestServer]], to be launched by
 * [[org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler]].
 * @param jarUrl URL to the application jar
 * @param mem Amount of memory for the driver
 * @param cores Number of cores for the driver
 * @param supervise Supervise the driver for long running app
 * @param command The command to launch the driver.
 * @param schedulerProperties Extra properties to pass the Mesos scheduler
 */
private[spark] class MesosDriverDescription(
    val name: String,
    val jarUrl: String,
    val mem: Int,
    val cores: Double,
    val supervise: Boolean,
    val command: Command,
    schedulerProperties: Map[String, String],
    val submissionId: String,
    val submissionDate: Date,
    val retryState: Option[MesosClusterRetryState] = None)
  extends Serializable {

  val conf = new SparkConf(false)
  schedulerProperties.foreach {case (k, v) => conf.set(k, v)}

  def copy(
      name: String = name,
      jarUrl: String = jarUrl,
      mem: Int = mem,
      cores: Double = cores,
      supervise: Boolean = supervise,
      command: Command = command,
      schedulerProperties: Map[String, String] = conf.getAll.toMap,
      submissionId: String = submissionId,
      submissionDate: Date = submissionDate,
      retryState: Option[MesosClusterRetryState] = retryState): MesosDriverDescription = {
    val conf = new SparkConf(false)
    schedulerProperties.foreach {case (k, v) => conf.set(k, v)}

    new MesosDriverDescription(name, jarUrl, mem, cores, supervise, command, conf.getAll.toMap,
      submissionId, submissionDate, retryState)
  }

  override def toString: String = s"MesosDriverDescription (${command.mainClass})"
}
