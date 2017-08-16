/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends SparkFunSuite with BeforeAndAfter with Logging {

  private val batchDuration: Duration = Milliseconds(1000)
  private val numBatches = 30
  private var directory: File = null

  before {
    directory = Utils.createTempDir()
  }

  after {
    if (directory != null) {
      Utils.deleteRecursively(directory)
    }
    StreamingContext.getActive().foreach { _.stop() }

    // Stop SparkContext if active
    SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("bla")).stop()
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(directory.getAbsolutePath, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory.getAbsolutePath, numBatches, batchDuration)
  }
}

