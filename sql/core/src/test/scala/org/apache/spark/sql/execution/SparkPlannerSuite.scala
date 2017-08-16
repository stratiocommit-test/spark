/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, LogicalPlan, ReturnAnswer, Union}
import org.apache.spark.sql.test.SharedSQLContext

class SparkPlannerSuite extends SharedSQLContext {
  import testImplicits._

  test("Ensure to go down only the first branch, not any other possible branches") {

    case object NeverPlanned extends LeafNode {
      override def output: Seq[Attribute] = Nil
    }

    var planned = 0
    object TestStrategy extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case ReturnAnswer(child) =>
          planned += 1
          planLater(child) :: planLater(NeverPlanned) :: Nil
        case Union(children) =>
          planned += 1
          UnionExec(children.map(planLater)) :: planLater(NeverPlanned) :: Nil
        case LocalRelation(output, data) =>
          planned += 1
          LocalTableScanExec(output, data) :: planLater(NeverPlanned) :: Nil
        case NeverPlanned =>
          fail("QueryPlanner should not go down to this branch.")
        case _ => Nil
      }
    }

    try {
      spark.experimental.extraStrategies = TestStrategy :: Nil

      val ds = Seq("a", "b", "c").toDS().union(Seq("d", "e", "f").toDS())

      assert(ds.collect().toSeq === Seq("a", "b", "c", "d", "e", "f"))
      assert(planned === 4)
    } finally {
      spark.experimental.extraStrategies = Nil
    }
  }
}
