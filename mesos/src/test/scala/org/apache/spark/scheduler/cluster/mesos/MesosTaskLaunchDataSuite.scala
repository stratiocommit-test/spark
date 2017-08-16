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

import org.apache.spark.SparkFunSuite

class MesosTaskLaunchDataSuite extends SparkFunSuite {
  test("serialize and deserialize data must be same") {
    val serializedTask = ByteBuffer.allocate(40)
    (Range(100, 110).map(serializedTask.putInt(_)))
    serializedTask.rewind
    val attemptNumber = 100
    val byteString = MesosTaskLaunchData(serializedTask, attemptNumber).toByteString
    serializedTask.rewind
    val mesosTaskLaunchData = MesosTaskLaunchData.fromByteString(byteString)
    assert(mesosTaskLaunchData.attemptNumber == attemptNumber)
    assert(mesosTaskLaunchData.serializedTask.equals(serializedTask))
  }
}
