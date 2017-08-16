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

import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Base class for writing tests for individual physical operators. For an example of how this
 * class's test helper methods can be used, see [[SortSuite]].
 */
private[sql] abstract class SparkPlanTest extends SparkFunSuite {
  protected def spark: SparkSession

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      input :: Nil,
      (plans: Seq[SparkPlan]) => planFunction(plans.head),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param left the left input data to be used.
   * @param right the right input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer2(
      left: DataFrame,
      right: DataFrame,
      planFunction: (SparkPlan, SparkPlan) => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      left :: right :: Nil,
      (plans: Seq[SparkPlan]) => planFunction(plans(0), plans(1)),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts a sequence of input SparkPlans and uses them to
   *                     instantiate the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def doCheckAnswer(
      input: Seq[DataFrame],
      planFunction: Seq[SparkPlan] => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    SparkPlanTest
      .checkAnswer(input, planFunction, expectedAnswer, sortAnswers, spark.sqlContext) match {
        case Some(errorMessage) => fail(errorMessage)
        case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkThatPlansAgree(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedPlanFunction: SparkPlan => SparkPlan,
      sortAnswers: Boolean = true): Unit = {
    SparkPlanTest.checkAnswer(
        input, planFunction, expectedPlanFunction, sortAnswers, spark.sqlContext) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}

/**
 * Helper methods for writing tests of individual physical operators.
 */
object SparkPlanTest {

  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   */
  def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedPlanFunction: SparkPlan => SparkPlan,
      sortAnswers: Boolean,
      spark: SQLContext): Option[String] = {

    val outputPlan = planFunction(input.queryExecution.sparkPlan)
    val expectedOutputPlan = expectedPlanFunction(input.queryExecution.sparkPlan)

    val expectedAnswer: Seq[Row] = try {
      executePlan(expectedOutputPlan, spark)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan to calculate expected answer:
             | $expectedOutputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    val actualAnswer: Seq[Row] = try {
      executePlan(outputPlan, spark)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan:
             | $outputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    SQLTestUtils.compareAnswers(actualAnswer, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
         | Results do not match.
         | Actual result Spark plan:
         | $outputPlan
         | Expected result Spark plan:
         | $expectedOutputPlan
         | $errorMessage
       """.stripMargin
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  def checkAnswer(
      input: Seq[DataFrame],
      planFunction: Seq[SparkPlan] => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean,
      spark: SQLContext): Option[String] = {

    val outputPlan = planFunction(input.map(_.queryExecution.sparkPlan))

    val sparkAnswer: Seq[Row] = try {
      executePlan(outputPlan, spark)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan:
             | $outputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    SQLTestUtils.compareAnswers(sparkAnswer, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
         | Results do not match for Spark plan:
         | $outputPlan
         | $errorMessage
       """.stripMargin
    }
  }

  /**
   * Runs the plan
   * @param outputPlan SparkPlan to be executed
   * @param spark SqlContext used for execution of the plan
   */
  def executePlan(outputPlan: SparkPlan, spark: SQLContext): Seq[Row] = {
    val execution = new QueryExecution(spark.sparkSession, null) {
      override lazy val sparkPlan: SparkPlan = outputPlan transform {
        case plan: SparkPlan =>
          val inputMap = plan.children.flatMap(_.output).map(a => (a.name, a)).toMap
          plan transformExpressions {
            case UnresolvedAttribute(Seq(u)) =>
              inputMap.getOrElse(u,
                sys.error(s"Invalid Test: Cannot resolve $u given input $inputMap"))
          }
      }
    }
    execution.executedPlan.executeCollectPublic().toSeq
  }
}

