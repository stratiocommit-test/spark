/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.streaming

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.{Clock, ManualClock, SystemClock}

class ProcessingTimeExecutorSuite extends SparkFunSuite {

  test("nextBatchTime") {
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(100))
    assert(processingTimeExecutor.nextBatchTime(0) === 100)
    assert(processingTimeExecutor.nextBatchTime(1) === 100)
    assert(processingTimeExecutor.nextBatchTime(99) === 100)
    assert(processingTimeExecutor.nextBatchTime(100) === 200)
    assert(processingTimeExecutor.nextBatchTime(101) === 200)
    assert(processingTimeExecutor.nextBatchTime(150) === 200)
  }

  test("calling nextBatchTime with the result of a previous call should return the next interval") {
    val intervalMS = 100
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMS))

    val ITERATION = 10
    var nextBatchTime: Long = 0
    for (it <- 1 to ITERATION) {
      nextBatchTime = processingTimeExecutor.nextBatchTime(nextBatchTime)
    }

    // nextBatchTime should be 1000
    assert(nextBatchTime === intervalMS * ITERATION)
  }

  private def testBatchTermination(intervalMs: Long): Unit = {
    var batchCounts = 0
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMs))
    processingTimeExecutor.execute(() => {
      batchCounts += 1
      // If the batch termination works well, batchCounts should be 3 after `execute`
      batchCounts < 3
    })
    assert(batchCounts === 3)
  }

  test("batch termination") {
    testBatchTermination(0)
    testBatchTermination(10)
  }

  test("notifyBatchFallingBehind") {
    val clock = new ManualClock()
    @volatile var batchFallingBehindCalled = false
    val latch = new CountDownLatch(1)
    val t = new Thread() {
      override def run(): Unit = {
        val processingTimeExecutor = new ProcessingTimeExecutor(ProcessingTime(100), clock) {
          override def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
            batchFallingBehindCalled = true
          }
        }
        processingTimeExecutor.execute(() => {
          latch.countDown()
          clock.waitTillTime(200)
          false
        })
      }
    }
    t.start()
    // Wait until the batch is running so that we don't call `advance` too early
    assert(latch.await(10, TimeUnit.SECONDS), "the batch has not yet started in 10 seconds")
    clock.advance(200)
    t.join()
    assert(batchFallingBehindCalled === true)
  }
}
