/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.util.Random

import com.google.common.util.concurrent.Uninterruptibles

import org.apache.spark.SparkFunSuite

class UninterruptibleThreadSuite extends SparkFunSuite {

  /** Sleep millis and return true if it's interrupted */
  private def sleep(millis: Long): Boolean = {
    try {
      Thread.sleep(millis)
      false
    } catch {
      case _: InterruptedException =>
        true
    }
  }

  test("interrupt when runUninterruptibly is running") {
    val enterRunUninterruptibly = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        runUninterruptibly {
          enterRunUninterruptibly.countDown()
          hasInterruptedException = sleep(1000)
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    assert(enterRunUninterruptibly.await(10, TimeUnit.SECONDS), "await timeout")
    t.interrupt()
    t.join()
    assert(hasInterruptedException === false)
    assert(interruptStatusBeforeExit === true)
  }

  test("interrupt before runUninterruptibly runs") {
    val interruptLatch = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        Uninterruptibles.awaitUninterruptibly(interruptLatch, 10, TimeUnit.SECONDS)
        try {
          runUninterruptibly {
            assert(false, "Should not reach here")
          }
        } catch {
          case _: InterruptedException => hasInterruptedException = true
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    t.interrupt()
    interruptLatch.countDown()
    t.join()
    assert(hasInterruptedException === true)
    assert(interruptStatusBeforeExit === false)
  }

  test("nested runUninterruptibly") {
    val enterRunUninterruptibly = new CountDownLatch(1)
    val interruptLatch = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        runUninterruptibly {
          enterRunUninterruptibly.countDown()
          Uninterruptibles.awaitUninterruptibly(interruptLatch, 10, TimeUnit.SECONDS)
          hasInterruptedException = sleep(1)
          runUninterruptibly {
            if (sleep(1)) {
              hasInterruptedException = true
            }
          }
          if (sleep(1)) {
            hasInterruptedException = true
          }
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    assert(enterRunUninterruptibly.await(10, TimeUnit.SECONDS), "await timeout")
    t.interrupt()
    interruptLatch.countDown()
    t.join()
    assert(hasInterruptedException === false)
    assert(interruptStatusBeforeExit === true)
  }

  test("stress test") {
    @volatile var hasInterruptedException = false
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        for (i <- 0 until 100) {
          try {
            runUninterruptibly {
              if (sleep(Random.nextInt(10))) {
                hasInterruptedException = true
              }
              runUninterruptibly {
                if (sleep(Random.nextInt(10))) {
                  hasInterruptedException = true
                }
              }
              if (sleep(Random.nextInt(10))) {
                hasInterruptedException = true
              }
            }
            Uninterruptibles.sleepUninterruptibly(Random.nextInt(10), TimeUnit.MILLISECONDS)
            // 50% chance to clear the interrupted status
            if (Random.nextBoolean()) {
              Thread.interrupted()
            }
          } catch {
            case _: InterruptedException =>
              // The first runUninterruptibly may throw InterruptedException if the interrupt status
              // is set before running `f`.
          }
        }
      }
    }
    t.start()
    for (i <- 0 until 400) {
      Thread.sleep(Random.nextInt(10))
      t.interrupt()
    }
    t.join()
    assert(hasInterruptedException === false)
  }
}
