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

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class TakeOrderedAndProjectSuite extends SparkPlanTest with SharedSQLContext {

  private var rand: Random = _
  private var seed: Long = 0

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    seed = System.currentTimeMillis()
    rand = new Random(seed)
  }

  private def generateRandomInputData(): DataFrame = {
    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", IntegerType, nullable = false)
    val inputData = Seq.fill(10000)(Row(rand.nextInt(), rand.nextInt()))
    spark.createDataFrame(sparkContext.parallelize(Random.shuffle(inputData), 10), schema)
  }

  /**
   * Adds a no-op filter to the child plan in order to prevent executeCollect() from being
   * called directly on the child plan.
   */
  private def noOpFilter(plan: SparkPlan): SparkPlan = FilterExec(Literal(true), plan)

  val limit = 250
  val sortOrder = 'a.desc :: 'b.desc :: Nil

  test("TakeOrderedAndProject.doExecute without project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(TakeOrderedAndProjectExec(limit, sortOrder, input.output, input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              SortExec(sortOrder, true, input))),
        sortAnswers = false)
    }
  }

  test("TakeOrderedAndProject.doExecute with project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(
            TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output.last), input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              ProjectExec(Seq(input.output.last),
                SortExec(sortOrder, true, input)))),
        sortAnswers = false)
    }
  }
}
