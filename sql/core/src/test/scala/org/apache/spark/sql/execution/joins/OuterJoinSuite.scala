/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

class OuterJoinSuite extends SparkPlanTest with SharedSQLContext {

  private lazy val left = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(2, 100.0),
      Row(2, 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, 1.0),
      Row(3, 3.0),
      Row(5, 1.0),
      Row(6, 6.0),
      Row(null, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  private lazy val right = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(0, 0.0),
      Row(2, 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, -1.0),
      Row(2, -1.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(5, 3.0),
      Row(7, 7.0),
      Row(null, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val condition = {
    And((left.col("a") === right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testOuterJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      joinType: JoinType,
      condition: => Expression,
      expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition))
      ExtractEquiJoinKeys.unapply(join)
    }

    if (joinType != FullOuter) {
      test(s"$testName using ShuffledHashJoin") {
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            val buildSide = if (joinType == LeftOuter) BuildRight else BuildLeft
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements(spark.sessionState.conf).apply(
                ShuffledHashJoinExec(
                  leftKeys, rightKeys, joinType, buildSide, boundCondition, left, right)),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }

    if (joinType != FullOuter) {
      test(s"$testName using BroadcastHashJoin") {
        val buildSide = joinType match {
          case LeftOuter => BuildRight
          case RightOuter => BuildLeft
          case _ => fail(s"Unsupported join type $joinType")
        }
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, buildSide, boundCondition, left, right),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }

    test(s"$testName using SortMergeJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(spark.sessionState.conf).apply(
              SortMergeJoinExec(leftKeys, rightKeys, joinType, boundCondition, left, right)),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build left") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildLeft, joinType, Some(condition)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build right") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildRight, joinType, Some(condition)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }

  // --- Basic outer joins ------------------------------------------------------------------------

  testOuterJoin(
    "basic left outer join",
    left,
    right,
    LeftOuter,
    condition,
    Seq(
      (null, null, null, null),
      (1, 2.0, null, null),
      (2, 100.0, null, null),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null),
      (5, 1.0, 5, 3.0),
      (6, 6.0, null, null)
    )
  )

  testOuterJoin(
    "basic right outer join",
    left,
    right,
    RightOuter,
    condition,
    Seq(
      (null, null, null, null),
      (null, null, 0, 0.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (null, null, 2, -1.0),
      (null, null, 2, -1.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0),
      (5, 1.0, 5, 3.0),
      (null, null, 7, 7.0)
    )
  )

  testOuterJoin(
    "basic full outer join",
    left,
    right,
    FullOuter,
    condition,
    Seq(
      (1, 2.0, null, null),
      (null, null, 2, -1.0),
      (null, null, 2, -1.0),
      (2, 100.0, null, null),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null),
      (5, 1.0, 5, 3.0),
      (6, 6.0, null, null),
      (null, null, 0, 0.0),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0),
      (null, null, 7, 7.0),
      (null, null, null, null),
      (null, null, null, null)
    )
  )

  // --- Both inputs empty ------------------------------------------------------------------------

  testOuterJoin(
    "left outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    LeftOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "right outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    RightOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "full outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    FullOuter,
    condition,
    Seq.empty
  )
}
