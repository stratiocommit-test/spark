/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{InterpretedMutableProjection, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning}

class PartitioningSuite extends SparkFunSuite {
  test("HashPartitioning compatibility should be sensitive to expression ordering (SPARK-9785)") {
    val expressions = Seq(Literal(2), Literal(3))
    // Consider two HashPartitionings that have the same _set_ of hash expressions but which are
    // created with different orderings of those expressions:
    val partitioningA = HashPartitioning(expressions, 100)
    val partitioningB = HashPartitioning(expressions.reverse, 100)
    // These partitionings are not considered equal:
    assert(partitioningA != partitioningB)
    // However, they both satisfy the same clustered distribution:
    val distribution = ClusteredDistribution(expressions)
    assert(partitioningA.satisfies(distribution))
    assert(partitioningB.satisfies(distribution))
    // These partitionings compute different hashcodes for the same input row:
    def computeHashCode(partitioning: HashPartitioning): Int = {
      val hashExprProj = new InterpretedMutableProjection(partitioning.expressions, Seq.empty)
      hashExprProj.apply(InternalRow.empty).hashCode()
    }
    assert(computeHashCode(partitioningA) != computeHashCode(partitioningB))
    // Thus, these partitionings are incompatible:
    assert(!partitioningA.compatibleWith(partitioningB))
    assert(!partitioningB.compatibleWith(partitioningA))
    assert(!partitioningA.guarantees(partitioningB))
    assert(!partitioningB.guarantees(partitioningA))

    // Just to be sure that we haven't cheated by having these methods always return false,
    // check that identical partitionings are still compatible with and guarantee each other:
    assert(partitioningA === partitioningA)
    assert(partitioningA.guarantees(partitioningA))
    assert(partitioningA.compatibleWith(partitioningA))
  }
}
