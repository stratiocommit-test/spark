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

import org.apache.spark.util.collection.SortDataFormat

/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the edge.
 *
 * @tparam ED type of the edge attribute
 *
 * @param srcId The vertex id of the source vertex
 * @param dstId The vertex id of the target vertex
 * @param attr The attribute associated with the edge
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var attr: ED = null.asInstanceOf[ED])
  extends Serializable {

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the id of the other vertex on the edge.
   */
  def otherVertexId(vid: VertexId): VertexId =
    if (srcId == vid) dstId else { assert(dstId == vid); srcId }

  /**
   * Return the relative direction of the edge to the corresponding
   * vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding
   * vertex.
   */
  def relativeDirection(vid: VertexId): EdgeDirection =
    if (vid == srcId) EdgeDirection.Out else { assert(vid == dstId); EdgeDirection.In }
}

object Edge {
  private[graphx] def lexicographicOrdering[ED] = new Ordering[Edge[ED]] {
    override def compare(a: Edge[ED], b: Edge[ED]): Int = {
      if (a.srcId == b.srcId) {
        if (a.dstId == b.dstId) 0
        else if (a.dstId < b.dstId) -1
        else 1
      } else if (a.srcId < b.srcId) -1
      else 1
    }
  }

  private[graphx] def edgeArraySortDataFormat[ED] = new SortDataFormat[Edge[ED], Array[Edge[ED]]] {
    override def getKey(data: Array[Edge[ED]], pos: Int): Edge[ED] = {
      data(pos)
    }

    override def swap(data: Array[Edge[ED]], pos0: Int, pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def copyElement(
        src: Array[Edge[ED]], srcPos: Int,
        dst: Array[Edge[ED]], dstPos: Int) {
      dst(dstPos) = src(srcPos)
    }

    override def copyRange(
        src: Array[Edge[ED]], srcPos: Int,
        dst: Array[Edge[ED]], dstPos: Int, length: Int) {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    override def allocate(length: Int): Array[Edge[ED]] = {
      new Array[Edge[ED]](length)
    }
  }
}
