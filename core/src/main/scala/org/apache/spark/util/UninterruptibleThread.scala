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

import javax.annotation.concurrent.GuardedBy

/**
 * A special Thread that provides "runUninterruptibly" to allow running codes without being
 * interrupted by `Thread.interrupt()`. If `Thread.interrupt()` is called during runUninterruptibly
 * is running, it won't set the interrupted status. Instead, setting the interrupted status will be
 * deferred until it's returning from "runUninterruptibly".
 *
 * Note: "runUninterruptibly" should be called only in `this` thread.
 */
private[spark] class UninterruptibleThread(name: String) extends Thread(name) {

  /** A monitor to protect "uninterruptible" and "interrupted" */
  private val uninterruptibleLock = new Object

  /**
   * Indicates if `this`  thread are in the uninterruptible status. If so, interrupting
   * "this" will be deferred until `this`  enters into the interruptible status.
   */
  @GuardedBy("uninterruptibleLock")
  private var uninterruptible = false

  /**
   * Indicates if we should interrupt `this` when we are leaving the uninterruptible zone.
   */
  @GuardedBy("uninterruptibleLock")
  private var shouldInterruptThread = false

  /**
   * Run `f` uninterruptibly in `this` thread. The thread won't be interrupted before returning
   * from `f`.
   *
   * If this method finds that `interrupt` is called before calling `f` and it's not inside another
   * `runUninterruptibly`, it will throw `InterruptedException`.
   *
   * Note: this method should be called only in `this` thread.
   */
  def runUninterruptibly[T](f: => T): T = {
    if (Thread.currentThread() != this) {
      throw new IllegalStateException(s"Call runUninterruptibly in a wrong thread. " +
        s"Expected: $this but was ${Thread.currentThread()}")
    }

    if (uninterruptibleLock.synchronized { uninterruptible }) {
      // We are already in the uninterruptible status. So just run "f" and return
      return f
    }

    uninterruptibleLock.synchronized {
      // Clear the interrupted status if it's set.
      if (Thread.interrupted() || shouldInterruptThread) {
        shouldInterruptThread = false
        // Since it's interrupted, we don't need to run `f` which may be a long computation.
        // Throw InterruptedException as we don't have a T to return.
        throw new InterruptedException()
      }
      uninterruptible = true
    }
    try {
      f
    } finally {
      uninterruptibleLock.synchronized {
        uninterruptible = false
        if (shouldInterruptThread) {
          // Recover the interrupted status
          super.interrupt()
          shouldInterruptThread = false
        }
      }
    }
  }

  /**
   * Interrupt `this` thread if possible. If `this` is in the uninterruptible status, it won't be
   * interrupted until it enters into the interruptible status.
   */
  override def interrupt(): Unit = {
    uninterruptibleLock.synchronized {
      if (uninterruptible) {
        shouldInterruptThread = true
      } else {
        super.interrupt()
      }
    }
  }
}
