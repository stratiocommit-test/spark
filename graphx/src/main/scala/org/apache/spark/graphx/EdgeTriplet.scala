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

/**
 * An edge triplet represents an edge along with the vertex attributes of its neighboring vertices.
 *
 * @tparam VD the type of the vertex attribute.
 * @tparam ED the type of the edge attribute
 */
class EdgeTriplet[VD, ED] extends Edge[ED] {
  /**
   * The source vertex attribute
   */
  var srcAttr: VD = _ // nullValue[VD]

  /**
   * The destination vertex attribute
   */
  var dstAttr: VD = _ // nullValue[VD]

  /**
   * Set the edge properties of this triplet.
   */
  protected[spark] def set(other: Edge[ED]): EdgeTriplet[VD, ED] = {
    srcId = other.srcId
    dstId = other.dstId
    attr = other.attr
    this
  }

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge
   * @return the attribute for the other vertex on the edge
   */
  def otherVertexAttr(vid: VertexId): VD =
    if (srcId == vid) dstAttr else { assert(dstId == vid); srcAttr }

  /**
   * Get the vertex object for the given vertex in the edge.
   *
   * @param vid the id of one of the two vertices on the edge
   * @return the attr for the vertex with that id
   */
  def vertexAttr(vid: VertexId): VD =
    if (srcId == vid) srcAttr else { assert(dstId == vid); dstAttr }

  override def toString: String = ((srcId, srcAttr), (dstId, dstAttr), attr).toString()

  def toTuple: ((VertexId, VD), (VertexId, VD), ED) = ((srcId, srcAttr), (dstId, dstAttr), attr)
}
