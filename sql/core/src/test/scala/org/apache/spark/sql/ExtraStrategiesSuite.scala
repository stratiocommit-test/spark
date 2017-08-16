/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSQLContext

case class FastOperator(output: Seq[Attribute]) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val str = Literal("so fast").value
    val row = new GenericInternalRow(Array[Any](str))
    val unsafeProj = UnsafeProjection.create(schema)
    val unsafeRow = unsafeProj(row).copy()
    sparkContext.parallelize(Seq(unsafeRow))
  }

  override def producedAttributes: AttributeSet = outputSet
  override def children: Seq[SparkPlan] = Nil
}

object TestStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(Seq(attr), _) if attr.name == "a" =>
      FastOperator(attr.toAttribute :: Nil) :: Nil
    case _ => Nil
  }
}

class ExtraStrategiesSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("insert an extraStrategy") {
    try {
      spark.experimental.extraStrategies = TestStrategy :: Nil

      val df = sparkContext.parallelize(Seq(("so slow", 1))).toDF("a", "b")
      checkAnswer(
        df.select("a"),
        Row("so fast"))

      checkAnswer(
        df.select("a", "b"),
        Row("so slow", 1))
    } finally {
      spark.experimental.extraStrategies = Nil
    }
  }
}
