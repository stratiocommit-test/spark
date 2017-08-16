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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, TypedFilter}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.BooleanType

class TypedFilterOptimizationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("EliminateSerialization", FixedPoint(50),
        EliminateSerialization) ::
      Batch("CombineTypedFilters", FixedPoint(50),
        CombineTypedFilters) :: Nil
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("filter after serialize with the same object type") {
    val input = LocalRelation('_1.int, '_2.int)
    val f = (i: (Int, Int)) => i._1 > 0

    val query = input
      .deserialize[(Int, Int)]
      .serialize[(Int, Int)]
      .filter(f).analyze

    val optimized = Optimize.execute(query)

    val expected = input
      .deserialize[(Int, Int)]
      .where(callFunction(f, BooleanType, 'obj))
      .serialize[(Int, Int)].analyze

    comparePlans(optimized, expected)
  }

  test("filter after serialize with different object types") {
    val input = LocalRelation('_1.int, '_2.int)
    val f = (i: OtherTuple) => i._1 > 0

    val query = input
      .deserialize[(Int, Int)]
      .serialize[(Int, Int)]
      .filter(f).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("filter before deserialize with the same object type") {
    val input = LocalRelation('_1.int, '_2.int)
    val f = (i: (Int, Int)) => i._1 > 0

    val query = input
      .filter(f)
      .deserialize[(Int, Int)]
      .serialize[(Int, Int)].analyze

    val optimized = Optimize.execute(query)

    val expected = input
      .deserialize[(Int, Int)]
      .where(callFunction(f, BooleanType, 'obj))
      .serialize[(Int, Int)].analyze

    comparePlans(optimized, expected)
  }

  test("filter before deserialize with different object types") {
    val input = LocalRelation('_1.int, '_2.int)
    val f = (i: OtherTuple) => i._1 > 0

    val query = input
      .filter(f)
      .deserialize[(Int, Int)]
      .serialize[(Int, Int)].analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("back to back filter with the same object type") {
    val input = LocalRelation('_1.int, '_2.int)
    val f1 = (i: (Int, Int)) => i._1 > 0
    val f2 = (i: (Int, Int)) => i._2 > 0

    val query = input.filter(f1).filter(f2).analyze
    val optimized = Optimize.execute(query)
    assert(optimized.collect { case t: TypedFilter => t }.length == 1)
  }

  test("back to back filter with different object types") {
    val input = LocalRelation('_1.int, '_2.int)
    val f1 = (i: (Int, Int)) => i._1 > 0
    val f2 = (i: OtherTuple) => i._2 > 0

    val query = input.filter(f1).filter(f2).analyze
    val optimized = Optimize.execute(query)
    assert(optimized.collect { case t: TypedFilter => t }.length == 2)
  }
}
