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

import org.apache.spark.SparkFunSuite

class JVMObjectTrackerSuite extends SparkFunSuite {
  test("JVMObjectId does not take null IDs") {
    intercept[IllegalArgumentException] {
      JVMObjectId(null)
    }
  }

  test("JVMObjectTracker") {
    val tracker = new JVMObjectTracker
    assert(tracker.size === 0)
    withClue("an empty tracker can be cleared") {
      tracker.clear()
    }
    val none = JVMObjectId("none")
    assert(tracker.get(none) === None)
    intercept[NoSuchElementException] {
      tracker(JVMObjectId("none"))
    }

    val obj1 = new Object
    val id1 = tracker.addAndGetId(obj1)
    assert(id1 != null)
    assert(tracker.size === 1)
    assert(tracker.get(id1).get.eq(obj1))
    assert(tracker(id1).eq(obj1))

    val obj2 = new Object
    val id2 = tracker.addAndGetId(obj2)
    assert(id1 !== id2)
    assert(tracker.size === 2)
    assert(tracker(id2).eq(obj2))

    val Some(obj1Removed) = tracker.remove(id1)
    assert(obj1Removed.eq(obj1))
    assert(tracker.get(id1) === None)
    assert(tracker.size === 1)
    assert(tracker(id2).eq(obj2))

    val obj3 = new Object
    val id3 = tracker.addAndGetId(obj3)
    assert(tracker.size === 2)
    assert(id3 != id1)
    assert(id3 != id2)
    assert(tracker(id3).eq(obj3))

    tracker.clear()
    assert(tracker.size === 0)
    assert(tracker.get(id1) === None)
    assert(tracker.get(id2) === None)
    assert(tracker.get(id3) === None)
  }
}
