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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.{Range => MesosRange, Ranges, Scalar}
import org.apache.mesos.SchedulerDriver
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Mockito._

object Utils {
  def createOffer(
      offerId: String,
      slaveId: String,
      mem: Int,
      cpus: Int,
      ports: Option[(Long, Long)] = None,
      gpus: Int = 0): Offer = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpus))
    ports.foreach { resourcePorts =>
      builder.addResourcesBuilder()
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Ranges.newBuilder().addRange(MesosRange.newBuilder()
          .setBegin(resourcePorts._1).setEnd(resourcePorts._2).build()))
    }
    if (gpus > 0) {
      builder.addResourcesBuilder()
        .setName("gpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(gpus))
    }
    builder.setId(createOfferId(offerId))
      .setFrameworkId(FrameworkID.newBuilder()
        .setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host${slaveId}")
      .build()
  }

  def verifyTaskLaunched(driver: SchedulerDriver, offerId: String): List[TaskInfo] = {
    val captor = ArgumentCaptor.forClass(classOf[java.util.Collection[TaskInfo]])
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(createOfferId(offerId))),
      captor.capture())
    captor.getValue.asScala.toList
  }

  def createOfferId(offerId: String): OfferID = {
    OfferID.newBuilder().setValue(offerId).build()
  }

  def createSlaveId(slaveId: String): SlaveID = {
    SlaveID.newBuilder().setValue(slaveId).build()
  }

  def createExecutorId(executorId: String): ExecutorID = {
    ExecutorID.newBuilder().setValue(executorId).build()
  }

  def createTaskId(taskId: String): TaskID = {
    TaskID.newBuilder().setValue(taskId).build()
  }
}
