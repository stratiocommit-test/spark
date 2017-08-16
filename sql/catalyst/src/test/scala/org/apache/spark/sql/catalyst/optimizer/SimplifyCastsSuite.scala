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

import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class SimplifyCastsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyCasts", FixedPoint(50), SimplifyCasts) :: Nil
  }

  test("non-nullable element array to nullable element array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, false)))
    val plan = input.select('a.cast(ArrayType(IntegerType, true)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select('a.as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("nullable element to non-nullable element array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, true)))
    val plan = input.select('a.cast(ArrayType(IntegerType, false)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("non-nullable value map to nullable value map cast") {
    val input = LocalRelation('m.map(MapType(StringType, StringType, false)))
    val plan = input.select('m.cast(MapType(StringType, StringType, true))
      .as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select('m.as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("nullable value map to non-nullable value map cast") {
    val input = LocalRelation('m.map(MapType(StringType, StringType, true)))
    val plan = input.select('m.cast(MapType(StringType, StringType, false))
      .as("casted")).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }
}

