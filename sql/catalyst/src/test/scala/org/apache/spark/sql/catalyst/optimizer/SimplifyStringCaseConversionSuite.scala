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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules._

class SimplifyStringCaseConversionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Simplify CaseConversionExpressions", Once,
        SimplifyCaseConversionExpressions) :: Nil
  }

  val testRelation = LocalRelation('a.string)

  test("simplify UPPER(UPPER(str))") {
    val originalQuery =
      testRelation
        .select(Upper(Upper('a)) as 'u)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper('a) as 'u)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify UPPER(LOWER(str))") {
    val originalQuery =
      testRelation
        .select(Upper(Lower('a)) as 'u)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper('a) as 'u)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LOWER(UPPER(str))") {
    val originalQuery =
      testRelation
        .select(Lower(Upper('a)) as 'l)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower('a) as 'l)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LOWER(LOWER(str))") {
    val originalQuery =
      testRelation
        .select(Lower(Lower('a)) as 'l)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower('a) as 'l)
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
