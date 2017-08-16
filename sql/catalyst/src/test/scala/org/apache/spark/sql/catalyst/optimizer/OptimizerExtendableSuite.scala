/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This is a test for SPARK-7727 if the Optimizer is kept being extendable
 */
class OptimizerExtendableSuite extends SparkFunSuite {

  /**
   * Dummy rule for test batches
   */
  object DummyRule extends Rule[LogicalPlan] {
    def apply(p: LogicalPlan): LogicalPlan = p
  }

  /**
   * This class represents a dummy extended optimizer that takes the batches of the
   * Optimizer and adds custom ones.
   */
  class ExtendedOptimizer extends SimpleTestOptimizer {

    // rules set to DummyRule, would not be executed anyways
    val myBatches: Seq[Batch] = {
      Batch("once", Once,
        DummyRule) ::
      Batch("fixedPoint", FixedPoint(100),
        DummyRule) :: Nil
    }

    override def batches: Seq[Batch] = super.batches ++ myBatches
  }

  test("Extending batches possible") {
    // test simply instantiates the new extended optimizer
    val extendedOptimizer = new ExtendedOptimizer()
  }
}
