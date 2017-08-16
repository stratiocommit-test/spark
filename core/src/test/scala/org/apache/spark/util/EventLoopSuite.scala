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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts

import org.apache.spark.SparkFunSuite

class EventLoopSuite extends SparkFunSuite with Timeouts {

  test("EventLoop") {
    val buffer = new ConcurrentLinkedQueue[Int]
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        buffer.add(event)
      }

      override def onError(e: Throwable): Unit = {}
    }
    eventLoop.start()
    (1 to 100).foreach(eventLoop.post)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert((1 to 100) === buffer.asScala.toSeq)
    }
    eventLoop.stop()
  }

  test("EventLoop: start and stop") {
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {}

      override def onError(e: Throwable): Unit = {}
    }
    assert(false === eventLoop.isActive)
    eventLoop.start()
    assert(true === eventLoop.isActive)
    eventLoop.stop()
    assert(false === eventLoop.isActive)
  }

  test("EventLoop: onError") {
    val e = new RuntimeException("Oops")
    @volatile var receivedError: Throwable = null
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        throw e
      }

      override def onError(e: Throwable): Unit = {
        receivedError = e
      }
    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(e === receivedError)
    }
    eventLoop.stop()
  }

  test("EventLoop: error thrown from onError should not crash the event thread") {
    val e = new RuntimeException("Oops")
    @volatile var receivedError: Throwable = null
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        throw e
      }

      override def onError(e: Throwable): Unit = {
        receivedError = e
        throw new RuntimeException("Oops")
      }
    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(e === receivedError)
      assert(eventLoop.isActive)
    }
    eventLoop.stop()
  }

  test("EventLoop: calling stop multiple times should only call onStop once") {
    var onStopTimes = 0
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
      }

      override def onError(e: Throwable): Unit = {
      }

      override def onStop(): Unit = {
        onStopTimes += 1
      }
    }

    eventLoop.start()

    eventLoop.stop()
    eventLoop.stop()
    eventLoop.stop()

    assert(1 === onStopTimes)
  }

  test("EventLoop: post event in multiple threads") {
    @volatile var receivedEventsCount = 0
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        receivedEventsCount += 1
      }

      override def onError(e: Throwable): Unit = {
      }

    }
    eventLoop.start()

    val threadNum = 5
    val eventsFromEachThread = 100
    (1 to threadNum).foreach { _ =>
      new Thread() {
        override def run(): Unit = {
          (1 to eventsFromEachThread).foreach(eventLoop.post)
        }
      }.start()
    }

    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(threadNum * eventsFromEachThread === receivedEventsCount)
    }
    eventLoop.stop()
  }

  test("EventLoop: onReceive swallows InterruptException") {
    val onReceiveLatch = new CountDownLatch(1)
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        onReceiveLatch.countDown()
        try {
          Thread.sleep(5000)
        } catch {
          case ie: InterruptedException => // swallow
        }
      }

      override def onError(e: Throwable): Unit = {
      }

    }
    eventLoop.start()
    eventLoop.post(1)
    failAfter(5 seconds) {
      // Wait until we enter `onReceive`
      onReceiveLatch.await()
      eventLoop.stop()
    }
    assert(false === eventLoop.isActive)
  }

  test("EventLoop: stop in eventThread") {
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        stop()
      }

      override def onError(e: Throwable): Unit = {
      }

    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(!eventLoop.isActive)
    }
  }

  test("EventLoop: stop() in onStart should call onStop") {
    @volatile var onStopCalled: Boolean = false
    val eventLoop = new EventLoop[Int]("test") {

      override def onStart(): Unit = {
        stop()
      }

      override def onReceive(event: Int): Unit = {
      }

      override def onError(e: Throwable): Unit = {
      }

      override def onStop(): Unit = {
        onStopCalled = true
      }
    }
    eventLoop.start()
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(!eventLoop.isActive)
    }
    assert(onStopCalled)
  }

  test("EventLoop: stop() in onReceive should call onStop") {
    @volatile var onStopCalled: Boolean = false
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        stop()
      }

      override def onError(e: Throwable): Unit = {
      }

      override def onStop(): Unit = {
        onStopCalled = true
      }
    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(!eventLoop.isActive)
    }
    assert(onStopCalled)
  }

  test("EventLoop: stop() in onError should call onStop") {
    @volatile var onStopCalled: Boolean = false
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        throw new RuntimeException("Oops")
      }

      override def onError(e: Throwable): Unit = {
        stop()
      }

      override def onStop(): Unit = {
        onStopCalled = true
      }
    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(!eventLoop.isActive)
    }
    assert(onStopCalled)
  }
}
