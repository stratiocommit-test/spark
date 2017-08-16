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

class LabelPropagationSuite extends SparkFunSuite with LocalSparkContext {
  test("Label Propagation") {
    withSpark { sc =>
      // Construct a graph with two cliques connected by a single edge
      val n = 5
      val clique1 = for (u <- 0L until n; v <- 0L until n) yield Edge(u, v, 1)
      val clique2 = for (u <- 0L to n; v <- 0L to n) yield Edge(u + n, v + n, 1)
      val twoCliques = sc.parallelize(clique1 ++ clique2 :+ Edge(0L, n, 1))
      val graph = Graph.fromEdges(twoCliques, 1)
      // Run label propagation
      val labels = LabelPropagation.run(graph, n * 4).cache()

      // All vertices within a clique should have the same label
      val clique1Labels = labels.vertices.filter(_._1 < n).map(_._2).collect.toArray
      assert(clique1Labels.forall(_ == clique1Labels(0)))
      val clique2Labels = labels.vertices.filter(_._1 >= n).map(_._2).collect.toArray
      assert(clique2Labels.forall(_ == clique2Labels(0)))
      // The two cliques should have different labels
      assert(clique1Labels(0) != clique2Labels(0))
    }
  }
}
