/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}

/**
 * Performs a physical redistribution of the data.  Used when the consumer of the query
 * result have expectations about the distribution and ordering of partitioned input data.
 */
abstract class RedistributeData extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class SortPartitions(sortExpressions: Seq[SortOrder], child: LogicalPlan)
  extends RedistributeData

/**
 * This method repartitions data using [[Expression]]s into `numPartitions`, and receives
 * information about the number of partitions during execution. Used when a specific ordering or
 * distribution is expected by the consumer of the query result. Use [[Repartition]] for RDD-like
 * `coalesce` and `repartition`.
 * If `numPartitions` is not specified, the number of partitions will be the number set by
 * `spark.sql.shuffle.partitions`.
 */
case class RepartitionByExpression(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    numPartitions: Option[Int] = None) extends RedistributeData {
  numPartitions match {
    case Some(n) => require(n > 0, s"Number of partitions ($n) must be positive.")
    case None => // Ok
  }
}
