/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.r

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

/** JVM object ID wrapper */
private[r] case class JVMObjectId(id: String) {
  require(id != null, "Object ID cannot be null.")
}

/**
 * Counter that tracks JVM objects returned to R.
 * This is useful for referencing these objects in RPC calls.
 */
private[r] class JVMObjectTracker {

  private[this] val objMap = new ConcurrentHashMap[JVMObjectId, Object]()
  private[this] val objCounter = new AtomicInteger()

  /**
   * Returns the JVM object associated with the input key or None if not found.
   */
  final def get(id: JVMObjectId): Option[Object] = this.synchronized {
    if (objMap.containsKey(id)) {
      Some(objMap.get(id))
    } else {
      None
    }
  }

  /**
   * Returns the JVM object associated with the input key or throws an exception if not found.
   */
  @throws[NoSuchElementException]("if key does not exist.")
  final def apply(id: JVMObjectId): Object = {
    get(id).getOrElse(
      throw new NoSuchElementException(s"$id does not exist.")
    )
  }

  /**
   * Adds a JVM object to track and returns assigned ID, which is unique within this tracker.
   */
  final def addAndGetId(obj: Object): JVMObjectId = {
    val id = JVMObjectId(objCounter.getAndIncrement().toString)
    objMap.put(id, obj)
    id
  }

  /**
   * Removes and returns a JVM object with the specific ID from the tracker, or None if not found.
   */
  final def remove(id: JVMObjectId): Option[Object] = this.synchronized {
    if (objMap.containsKey(id)) {
      Some(objMap.remove(id))
    } else {
      None
    }
  }

  /**
   * Number of JVM objects being tracked.
   */
  final def size: Int = objMap.size()

  /**
   * Clears the tracker.
   */
  final def clear(): Unit = objMap.clear()
}
