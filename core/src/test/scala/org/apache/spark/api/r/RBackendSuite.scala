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

class RBackendSuite extends SparkFunSuite {
  test("close() clears jvmObjectTracker") {
    val backend = new RBackend
    val tracker = backend.jvmObjectTracker
    val id = tracker.addAndGetId(new Object)
    backend.close()
    assert(tracker.get(id) === None)
    assert(tracker.size === 0)
  }
}
