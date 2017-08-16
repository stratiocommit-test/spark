/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ManualClock

class RecurringTimerSuite extends SparkFunSuite with PrivateMethodTester {

  test("basic") {
    val clock = new ManualClock()
    val results = new ConcurrentLinkedQueue[Long]()
    val timer = new RecurringTimer(clock, 100, time => {
      results.add(time)
    }, "RecurringTimerSuite-basic")
    timer.start(0)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L))
    }
    clock.advance(100)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L, 100L))
    }
    clock.advance(200)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L, 100L, 200L, 300L))
    }
    assert(timer.stop(interruptTimer = true) === 300L)
  }

  test("SPARK-10224: call 'callback' after stopping") {
    val clock = new ManualClock()
    val results = new ConcurrentLinkedQueue[Long]
    val timer = new RecurringTimer(clock, 100, time => {
      results.add(time)
    }, "RecurringTimerSuite-SPARK-10224")
    timer.start(0)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L))
    }
    @volatile var lastTime = -1L
    // Now RecurringTimer is waiting for the next interval
    val thread = new Thread {
      override def run(): Unit = {
        lastTime = timer.stop(interruptTimer = false)
      }
    }
    thread.start()
    val stopped = PrivateMethod[RecurringTimer]('stopped)
    // Make sure the `stopped` field has been changed
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(timer.invokePrivate(stopped()) === true)
    }
    clock.advance(200)
    // When RecurringTimer is awake from clock.waitTillTime, it will call `callback` once.
    // Then it will find `stopped` is true and exit the loop, but it should call `callback` again
    // before exiting its internal thread.
    thread.join()
    assert(results.asScala.toSeq === Seq(0L, 100L, 200L))
    assert(lastTime === 200L)
  }
}
