/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import java.util.UUID
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Utility wrapper to "smuggle" objects into tasks while bypassing serialization.
 * This is intended for testing purposes, primarily to make locks, semaphores, and
 * other constructs that would not survive serialization available from within tasks.
 * A Smuggle reference is itself serializable, but after being serialized and
 * deserialized, it still refers to the same underlying "smuggled" object, as long
 * as it was deserialized within the same JVM. This can be useful for tests that
 * depend on the timing of task completion to be deterministic, since one can "smuggle"
 * a lock or semaphore into the task, and then the task can block until the test gives
 * the go-ahead to proceed via the lock.
 */
class Smuggle[T] private(val key: Symbol) extends Serializable {
  def smuggledObject: T = Smuggle.get(key)
}


object Smuggle {
  /**
   * Wraps the specified object to be smuggled into a serialized task without
   * being serialized itself.
   *
   * @param smuggledObject
   * @tparam T
   * @return Smuggle wrapper around smuggledObject.
   */
  def apply[T](smuggledObject: T): Smuggle[T] = {
    val key = Symbol(UUID.randomUUID().toString)
    lock.writeLock().lock()
    try {
      smuggledObjects += key -> smuggledObject
    } finally {
      lock.writeLock().unlock()
    }
    new Smuggle(key)
  }

  private val lock = new ReentrantReadWriteLock
  private val smuggledObjects = mutable.WeakHashMap.empty[Symbol, Any]

  private def get[T](key: Symbol) : T = {
    lock.readLock().lock()
    try {
      smuggledObjects(key).asInstanceOf[T]
    } finally {
      lock.readLock().unlock()
    }
  }

  /**
   * Implicit conversion of a Smuggle wrapper to the object being smuggled.
   *
   * @param smuggle the wrapper to unpack.
   * @tparam T
   * @return the smuggled object represented by the wrapper.
   */
  implicit def unpackSmuggledObject[T](smuggle : Smuggle[T]): T = smuggle.smuggledObject

}
