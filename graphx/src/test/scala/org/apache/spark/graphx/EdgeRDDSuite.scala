/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.graphx

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel

class EdgeRDDSuite extends SparkFunSuite with LocalSparkContext {

  test("cache, getStorageLevel") {
    // test to see if getStorageLevel returns correct value after caching
    withSpark { sc =>
      val verts = sc.parallelize(List((0L, 0), (1L, 1), (1L, 2), (2L, 3), (2L, 3), (2L, 3)))
      val edges = EdgeRDD.fromEdges(sc.parallelize(List.empty[Edge[Int]]))
      assert(edges.getStorageLevel == StorageLevel.NONE)
      edges.cache()
      assert(edges.getStorageLevel == StorageLevel.MEMORY_ONLY)
    }
  }

}
