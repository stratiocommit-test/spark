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
 * Represents an edge along with its neighboring vertices and allows sending messages along the
 * edge. Used in [[Graph#aggregateMessages]].
 */
abstract class EdgeContext[VD, ED, A] {
  /** The vertex id of the edge's source vertex. */
  def srcId: VertexId
  /** The vertex id of the edge's destination vertex. */
  def dstId: VertexId
  /** The vertex attribute of the edge's source vertex. */
  def srcAttr: VD
  /** The vertex attribute of the edge's destination vertex. */
  def dstAttr: VD
  /** The attribute associated with the edge. */
  def attr: ED

  /** Sends a message to the source vertex. */
  def sendToSrc(msg: A): Unit
  /** Sends a message to the destination vertex. */
  def sendToDst(msg: A): Unit

  /** Converts the edge and vertex properties into an [[EdgeTriplet]] for convenience. */
  def toEdgeTriplet: EdgeTriplet[VD, ED] = {
    val et = new EdgeTriplet[VD, ED]
    et.srcId = srcId
    et.srcAttr = srcAttr
    et.dstId = dstId
    et.dstAttr = dstAttr
    et.attr = attr
    et
  }
}

object EdgeContext {

  /**
   * Extractor mainly used for Graph#aggregateMessages*.
   * Example:
   * {{{
   *  val messages = graph.aggregateMessages(
   *    case ctx @ EdgeContext(_, _, _, _, attr) =>
   *      ctx.sendToDst(attr)
   *    , _ + _)
   * }}}
   */
  def unapply[VD, ED, A](edge: EdgeContext[VD, ED, A]): Some[(VertexId, VertexId, VD, VD, ED)] =
    Some(edge.srcId, edge.dstId, edge.srcAttr, edge.dstAttr, edge.attr)
}
