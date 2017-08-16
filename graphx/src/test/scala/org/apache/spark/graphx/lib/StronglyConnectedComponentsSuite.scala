/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._


class StronglyConnectedComponentsSuite extends SparkFunSuite with LocalSparkContext {

  test("Island Strongly Connected Components") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 5L).map(x => (x, -1)))
      val edges = sc.parallelize(Seq.empty[Edge[Int]])
      val graph = Graph(vertices, edges)
      val sccGraph = graph.stronglyConnectedComponents(5)
      for ((id, scc) <- sccGraph.vertices.collect()) {
        assert(id === scc)
      }
    }
  }

  test("Cycle Strongly Connected Components") {
    withSpark { sc =>
      val rawEdges = sc.parallelize((0L to 6L).map(x => (x, (x + 1) % 7)))
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = graph.stronglyConnectedComponents(20)
      for ((id, scc) <- sccGraph.vertices.collect()) {
        assert(0L === scc)
      }
    }
  }

  test("2 Cycle Strongly Connected Components") {
    withSpark { sc =>
      val edges =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(3L -> 4L, 4L -> 5L, 5L -> 3L) ++
        Array(6L -> 0L, 5L -> 7L)
      val rawEdges = sc.parallelize(edges)
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = graph.stronglyConnectedComponents(20)
      for ((id, scc) <- sccGraph.vertices.collect()) {
        if (id < 3) {
          assert(0L === scc)
        } else if (id < 6) {
          assert(3L === scc)
        } else {
          assert(id === scc)
        }
      }
    }
  }

}
