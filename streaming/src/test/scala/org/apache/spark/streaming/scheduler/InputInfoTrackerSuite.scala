/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

class InputInfoTrackerSuite extends SparkFunSuite with BeforeAndAfter {

  private var ssc: StreamingContext = _

  before {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectStreamTacker")
    if (ssc == null) {
      ssc = new StreamingContext(conf, Duration(1000))
    }
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("test report and get InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val streamId2 = 1
    val time = Time(0L)
    val inputInfo1 = StreamInputInfo(streamId1, 100L)
    val inputInfo2 = StreamInputInfo(streamId2, 300L)
    inputInfoTracker.reportInfo(time, inputInfo1)
    inputInfoTracker.reportInfo(time, inputInfo2)

    val batchTimeToInputInfos = inputInfoTracker.getInfo(time)
    assert(batchTimeToInputInfos.size == 2)
    assert(batchTimeToInputInfos.keys === Set(streamId1, streamId2))
    assert(batchTimeToInputInfos(streamId1) === inputInfo1)
    assert(batchTimeToInputInfos(streamId2) === inputInfo2)
    assert(inputInfoTracker.getInfo(time)(streamId1) === inputInfo1)
  }

  test("test cleanup InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val inputInfo1 = StreamInputInfo(streamId1, 100L)
    val inputInfo2 = StreamInputInfo(streamId1, 300L)
    inputInfoTracker.reportInfo(Time(0), inputInfo1)
    inputInfoTracker.reportInfo(Time(1), inputInfo2)

    inputInfoTracker.cleanup(Time(0))
    assert(inputInfoTracker.getInfo(Time(0))(streamId1) === inputInfo1)
    assert(inputInfoTracker.getInfo(Time(1))(streamId1) === inputInfo2)

    inputInfoTracker.cleanup(Time(1))
    assert(inputInfoTracker.getInfo(Time(0)).get(streamId1) === None)
    assert(inputInfoTracker.getInfo(Time(1))(streamId1) === inputInfo2)
  }
}
