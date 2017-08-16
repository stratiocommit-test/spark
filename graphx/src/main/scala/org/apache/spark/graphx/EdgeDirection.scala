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
 * The direction of a directed edge relative to a vertex.
 */
class EdgeDirection private (private val name: String) extends Serializable {
  /**
   * Reverse the direction of an edge.  An in becomes out,
   * out becomes in and both and either remain the same.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In => EdgeDirection.Out
    case EdgeDirection.Out => EdgeDirection.In
    case EdgeDirection.Either => EdgeDirection.Either
    case EdgeDirection.Both => EdgeDirection.Both
  }

  override def toString: String = "EdgeDirection." + name

  override def equals(o: Any): Boolean = o match {
    case other: EdgeDirection => other.name == name
    case _ => false
  }

  override def hashCode: Int = name.hashCode
}


/**
 * A set of [[EdgeDirection]]s.
 */
object EdgeDirection {
  /** Edges arriving at a vertex. */
  final val In: EdgeDirection = new EdgeDirection("In")

  /** Edges originating from a vertex. */
  final val Out: EdgeDirection = new EdgeDirection("Out")

  /** Edges originating from *or* arriving at a vertex of interest. */
  final val Either: EdgeDirection = new EdgeDirection("Either")

  /** Edges originating from *and* arriving at a vertex of interest. */
  final val Both: EdgeDirection = new EdgeDirection("Both")
}
