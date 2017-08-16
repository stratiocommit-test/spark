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

class EdgeSuite extends SparkFunSuite {
  test ("compare") {
    // descending order
    val testEdges: Array[Edge[Int]] = Array(
      Edge(0x7FEDCBA987654321L, -0x7FEDCBA987654321L, 1),
      Edge(0x2345L, 0x1234L, 1),
      Edge(0x1234L, 0x5678L, 1),
      Edge(0x1234L, 0x2345L, 1),
      Edge(-0x7FEDCBA987654321L, 0x7FEDCBA987654321L, 1)
    )
    // to ascending order
    val sortedEdges = testEdges.sorted(Edge.lexicographicOrdering[Int])

    for (i <- 0 until testEdges.length) {
      assert(sortedEdges(i) == testEdges(testEdges.length - i - 1))
    }
  }
}
