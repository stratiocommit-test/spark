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

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.graphx.Graph._

class GraphOpsSuite extends SparkFunSuite with LocalSparkContext {

  test("joinVertices") {
    withSpark { sc =>
      val vertices =
        sc.parallelize(Seq[(VertexId, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(VertexId, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: VertexId, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Either).cache()
      assert(nbrs.count === 100)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach {
        case (vid, nbrs) =>
          val s = nbrs.toSet
          assert(s.contains((vid + 1) % 100))
          assert(s.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("removeSelfEdges") {
    withSpark { sc =>
      val edgeArray = Array((1 -> 2), (2 -> 3), (3 -> 3), (4 -> 3), (1 -> 1))
        .map {
          case (a, b) => (a.toLong, b.toLong)
        }
      val correctEdges = edgeArray.filter { case (a, b) => a != b }.toSet
      val graph = Graph.fromEdgeTuples(sc.parallelize(edgeArray), 1)
      val canonicalizedEdges = graph.removeSelfEdges().edges.map(e => (e.srcId, e.dstId))
        .collect
      assert(canonicalizedEdges.toSet.size === canonicalizedEdges.size)
      assert(canonicalizedEdges.toSet === correctEdges)
    }
  }

  test ("filter") {
    withSpark { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x: VertexId, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filter(
        graph => {
          val degrees: VertexRDD[Int] = graph.outDegrees
          graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
        },
        vpred = (vid: VertexId, deg: Int) => deg > 0
      ).cache()

      val v = filteredGraph.vertices.collect().toSet
      assert(v === Set((0, 0)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = filteredGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e.isEmpty)
    }
  }

  test ("convertToCanonicalEdges") {
    withSpark { sc =>
      val vertices =
        sc.parallelize(Seq[(VertexId, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges =
        sc.parallelize(Seq(Edge(1, 2, 1), Edge(2, 1, 1), Edge(3, 2, 2)))
      val g: Graph[String, Int] = Graph(vertices, edges)

      val g1 = g.convertToCanonicalEdges()

      val e = g1.edges.collect().toSet
      assert(e === Set(Edge(1, 2, 1), Edge(2, 3, 2)))
    }
  }

  test("collectEdgesCycleDirectionOut") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.dstId)
          assert(edgeDstIds.contains((vid + 1) % 100))
      }
    }
  }

  test("collectEdgesCycleDirectionIn") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeSrcIds = s.map(e => e.srcId)
          assert(edgeSrcIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesCycleDirectionEither") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 2) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
          assert(edgeIds.contains((vid + 1) % 100))
          assert(edgeIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesChainDirectionOut") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.dstId)
          assert(edgeDstIds.contains(vid + 1))
      }
    }
  }

  test("collectEdgesChainDirectionIn") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.srcId)
          assert(edgeDstIds.contains((vid - 1) % 100))
      }
    }
  }

  test("collectEdgesChainDirectionEither") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count === 50)
      edges.collect.foreach {
        case (vid, edges) => if (vid > 0 && vid < 49) {
          assert(edges.size == 2)
        } else {
          assert(edges.size == 1)
        }
      }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
          if (vid == 0) { assert(edgeIds.contains(1)) }
          else if (vid == 49) { assert(edgeIds.contains(48)) }
          else {
            assert(edgeIds.contains(vid + 1))
            assert(edgeIds.contains(vid - 1))
          }
      }
    }
  }

  private def getCycleGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val cycle = (0 until numVertices).map(x => (x, (x + 1) % numVertices))
    getGraphFromSeq(sc, cycle)
  }

  private def getChainGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val chain = (0 until numVertices - 1).map(x => (x, (x + 1)))
    getGraphFromSeq(sc, chain)
  }

  private def getGraphFromSeq(sc: SparkContext, seq: IndexedSeq[(Int, Int)]): Graph[Double, Int] = {
    val rawEdges = sc.parallelize(seq, 3).map { case (s, d) => (s.toLong, d.toLong) }
    Graph.fromEdgeTuples(rawEdges, 1.0).cache()
  }
}
