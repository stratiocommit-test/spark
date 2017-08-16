/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.util._

/**
 * Tests for the sameResult function of [[LogicalPlan]].
 */
class SameResultSuite extends SparkFunSuite {
  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation2 = LocalRelation('a.int, 'b.int, 'c.int)

  def assertSameResult(a: LogicalPlan, b: LogicalPlan, result: Boolean = true): Unit = {
    val aAnalyzed = a.analyze
    val bAnalyzed = b.analyze

    if (aAnalyzed.sameResult(bAnalyzed) != result) {
      val comparison = sideBySide(aAnalyzed.toString, bAnalyzed.toString).mkString("\n")
      fail(s"Plans should return sameResult = $result\n$comparison")
    }
  }

  test("relations") {
    assertSameResult(testRelation, testRelation2)
  }

  test("projections") {
    assertSameResult(testRelation.select('a), testRelation2.select('a))
    assertSameResult(testRelation.select('b), testRelation2.select('b))
    assertSameResult(testRelation.select('a, 'b), testRelation2.select('a, 'b))
    assertSameResult(testRelation.select('b, 'a), testRelation2.select('b, 'a))

    assertSameResult(testRelation, testRelation2.select('a), result = false)
    assertSameResult(testRelation.select('b, 'a), testRelation2.select('a, 'b), result = false)
  }

  test("filters") {
    assertSameResult(testRelation.where('a === 'b), testRelation2.where('a === 'b))
  }

  test("sorts") {
    assertSameResult(testRelation.orderBy('a.asc), testRelation2.orderBy('a.asc))
  }

  test("union") {
    assertSameResult(Union(Seq(testRelation, testRelation2)),
      Union(Seq(testRelation2, testRelation)))
  }
}
