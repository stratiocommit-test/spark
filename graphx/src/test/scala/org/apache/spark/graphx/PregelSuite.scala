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

class PregelSuite extends SparkFunSuite with LocalSparkContext {

  test("1 iteration") {
    withSpark { sc =>
      val n = 5
      val starEdges = (1 to n).map(x => (0: VertexId, x: VertexId))
      val star = Graph.fromEdgeTuples(sc.parallelize(starEdges, 3), "v").cache()
      val result = Pregel(star, 0)(
        (vid, attr, msg) => attr,
        et => Iterator.empty,
        (a: Int, b: Int) => throw new Exception("mergeMsg run unexpectedly"))
      assert(result.vertices.collect.toSet === star.vertices.collect.toSet)
    }
  }

  test("chain propagation") {
    withSpark { sc =>
      val n = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: VertexId, x + 1: VertexId)), 3),
        0).cache()
      assert(chain.vertices.collect.toSet === (1 to n).map(x => (x: VertexId, 0)).toSet)
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid == 1) 1 else 0 }.cache()
      assert(chainWithSeed.vertices.collect.toSet ===
        Set((1: VertexId, 1)) ++ (2 to n).map(x => (x: VertexId, 0)).toSet)
      val result = Pregel(chainWithSeed, 0)(
        (vid, attr, msg) => math.max(msg, attr),
        et => if (et.dstAttr != et.srcAttr) Iterator((et.dstId, et.srcAttr)) else Iterator.empty,
        (a: Int, b: Int) => math.max(a, b))
      assert(result.vertices.collect.toSet ===
        chain.vertices.mapValues { (vid, attr) => attr + 1 }.collect.toSet)
    }
  }
}
