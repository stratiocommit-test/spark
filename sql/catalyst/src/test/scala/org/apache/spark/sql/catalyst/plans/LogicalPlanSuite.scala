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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

/**
 * This suite is used to test [[LogicalPlan]]'s `resolveOperators` and make sure it can correctly
 * skips sub-trees that have already been marked as analyzed.
 */
class LogicalPlanSuite extends SparkFunSuite {
  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val testRelation = LocalRelation()

  test("resolveOperator runs on operators") {
    invocationCount = 0
    val plan = Project(Nil, testRelation)
    plan resolveOperators function

    assert(invocationCount === 1)
  }

  test("resolveOperator runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan resolveOperators function

    assert(invocationCount === 2)
  }

  test("resolveOperator skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan.foreach(_.setAnalyzed())
    plan resolveOperators function

    assert(invocationCount === 0)
  }

  test("resolveOperator skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(Nil, testRelation)
    val plan2 = Project(Nil, plan1)
    plan1.foreach(_.setAnalyzed())
    plan2 resolveOperators function

    assert(invocationCount === 1)
  }

  test("isStreaming") {
    val relation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
    val incrementalRelation = new LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)())) {
      override def isStreaming(): Boolean = true
    }

    case class TestBinaryRelation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
      override def output: Seq[Attribute] = left.output ++ right.output
    }

    require(relation.isStreaming === false)
    require(incrementalRelation.isStreaming === true)
    assert(TestBinaryRelation(relation, relation).isStreaming === false)
    assert(TestBinaryRelation(incrementalRelation, relation).isStreaming === true)
    assert(TestBinaryRelation(relation, incrementalRelation).isStreaming === true)
    assert(TestBinaryRelation(incrementalRelation, incrementalRelation).isStreaming)
  }
}
