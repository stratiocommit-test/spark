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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.test.SharedSQLContext

class QueryExecutionSuite extends SharedSQLContext {
  test("toString() exception/error handling") {
    val badRule = new SparkStrategy {
      var mode: String = ""
      override def apply(plan: LogicalPlan): Seq[SparkPlan] = mode.toLowerCase match {
        case "exception" => throw new AnalysisException(mode)
        case "error" => throw new Error(mode)
        case _ => Nil
      }
    }
    spark.experimental.extraStrategies = badRule :: Nil

    def qe: QueryExecution = new QueryExecution(spark, OneRowRelation)

    // Nothing!
    badRule.mode = ""
    assert(qe.toString.contains("OneRowRelation"))

    // Throw an AnalysisException - this should be captured.
    badRule.mode = "exception"
    assert(qe.toString.contains("org.apache.spark.sql.AnalysisException"))

    // Throw an Error - this should not be captured.
    badRule.mode = "error"
    val error = intercept[Error](qe.toString)
    assert(error.getMessage.contains("error"))
  }
}
