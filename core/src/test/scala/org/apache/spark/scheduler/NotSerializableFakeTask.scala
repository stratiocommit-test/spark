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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.TaskContext

/**
 * A Task implementation that fails to serialize.
 */
private[spark] class NotSerializableFakeTask(myId: Int, stageId: Int)
  extends Task[Array[Byte]](stageId, 0, 0) {

  override def runTask(context: TaskContext): Array[Byte] = Array.empty[Byte]
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    if (stageId == 0) {
      throw new IllegalStateException("Cannot serialize")
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {}
}
