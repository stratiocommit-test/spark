/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.util.collection.BitSet

private[graphx] object VertexPartition {
  /** Construct a `VertexPartition` from the given vertices. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : VertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter)
    new VertexPartition(index, values, mask)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `VertexPartition`.
   */
  implicit def partitionToOps[VD: ClassTag](partition: VertexPartition[VD])
    : VertexPartitionOps[VD] = new VertexPartitionOps(partition)

  /**
   * Implicit evidence that `VertexPartition` is a member of the `VertexPartitionBaseOpsConstructor`
   * typeclass. This enables invoking `VertexPartitionBase` operations on a `VertexPartition` via an
   * evidence parameter, as in [[VertexPartitionBaseOps]].
   */
  implicit object VertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[VertexPartition] {
    def toOps[VD: ClassTag](partition: VertexPartition[VD])
      : VertexPartitionBaseOps[VD, VertexPartition] = partitionToOps(partition)
  }
}

/** A map from vertex id to vertex attribute. */
private[graphx] class VertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet)
  extends VertexPartitionBase[VD]

private[graphx] class VertexPartitionOps[VD: ClassTag](self: VertexPartition[VD])
  extends VertexPartitionBaseOps[VD, VertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): VertexPartition[VD] = {
    new VertexPartition(index, self.values, self.mask)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): VertexPartition[VD2] = {
    new VertexPartition(self.index, values, self.mask)
  }

  def withMask(mask: BitSet): VertexPartition[VD] = {
    new VertexPartition(self.index, self.values, mask)
  }
}
