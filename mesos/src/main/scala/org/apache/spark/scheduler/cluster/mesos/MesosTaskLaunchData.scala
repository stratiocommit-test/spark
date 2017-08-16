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

import java.nio.ByteBuffer

import org.apache.mesos.protobuf.ByteString

import org.apache.spark.internal.Logging

/**
 * Wrapper for serializing the data sent when launching Mesos tasks.
 */
private[spark] case class MesosTaskLaunchData(
  serializedTask: ByteBuffer,
  attemptNumber: Int) extends Logging {

  def toByteString: ByteString = {
    val dataBuffer = ByteBuffer.allocate(4 + serializedTask.limit)
    dataBuffer.putInt(attemptNumber)
    dataBuffer.put(serializedTask)
    dataBuffer.rewind
    logDebug(s"ByteBuffer size: [${dataBuffer.remaining}]")
    ByteString.copyFrom(dataBuffer)
  }
}

private[spark] object MesosTaskLaunchData extends Logging {
  def fromByteString(byteString: ByteString): MesosTaskLaunchData = {
    val byteBuffer = byteString.asReadOnlyByteBuffer()
    logDebug(s"ByteBuffer size: [${byteBuffer.remaining}]")
    val attemptNumber = byteBuffer.getInt // updates the position by 4 bytes
    val serializedTask = byteBuffer.slice() // subsequence starting at the current position
    MesosTaskLaunchData(serializedTask, attemptNumber)
  }
}
