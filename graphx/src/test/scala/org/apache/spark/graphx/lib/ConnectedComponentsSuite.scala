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
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._


class ConnectedComponentsSuite extends SparkFunSuite with LocalSparkContext {

  test("Grid Connected Components") {
    withSpark { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10)
      val ccGraph = gridGraph.connectedComponents()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum()
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Reverse Grid Connected Components") {
    withSpark { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).reverse
      val ccGraph = gridGraph.connectedComponents()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum()
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val chain2 = (10 until 20).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s, d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, 1.0)
      val ccGraph = twoChains.connectedComponents()
      val vertices = ccGraph.vertices.collect()
      for ( (id, cc) <- vertices ) {
        if (id < 10) {
          assert(cc === 0)
        } else {
          assert(cc === 10)
        }
      }
      val ccMap = vertices.toMap
      for (id <- 0 until 20) {
        if (id < 10) {
          assert(ccMap(id) === 0)
        } else {
          assert(ccMap(id) === 10)
        }
      }
    }
  } // end of chain connected components

  test("Reverse Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val chain2 = (10 until 20).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s, d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, true).reverse
      val ccGraph = twoChains.connectedComponents()
      val vertices = ccGraph.vertices.collect()
      for ( (id, cc) <- vertices ) {
        if (id < 10) {
          assert(cc === 0)
        } else {
          assert(cc === 10)
        }
      }
      val ccMap = vertices.toMap
      for ( id <- 0 until 20 ) {
        if (id < 10) {
          assert(ccMap(id) === 0)
        } else {
          assert(ccMap(id) === 10)
        }
      }
    }
  } // end of reverse chain connected components

  test("Connected Components on a Toy Connected Graph") {
    withSpark { sc =>
      // Create an RDD for the vertices
      val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                       (4L, ("peter", "student"))))
      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                       Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
      // Edges are:
      //   2 ---> 5 ---> 3
      //          | \
      //          V   \|
      //   4 ---> 0    7
      //
      // Define a default user in case there are relationship with missing user
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, relationships, defaultUser)
      val ccGraph = graph.connectedComponents()
      val vertices = ccGraph.vertices.collect()
      for ( (id, cc) <- vertices ) {
        assert(cc === 0)
      }
    }
  } // end of toy connected components

}
