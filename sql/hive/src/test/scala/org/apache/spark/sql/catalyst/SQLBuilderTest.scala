/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.test.TestHiveSingleton


abstract class SQLBuilderTest extends QueryTest with TestHiveSingleton {
  protected def checkSQL(e: Expression, expectedSQL: String): Unit = {
    val actualSQL = e.sql
    try {
      assert(actualSQL === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following expression:
             |
             |${e.prettyName}
             |
             |$cause
           """.stripMargin)
    }
  }

  protected def checkSQL(plan: LogicalPlan, expectedSQL: String): Unit = {
    val generatedSQL = try new SQLBuilder(plan).toSQL catch { case NonFatal(e) =>
      fail(
        s"""Cannot convert the following logical query plan to SQL:
           |
           |${plan.treeString}
         """.stripMargin)
    }

    try {
      assert(generatedSQL === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following logical query plan:
             |
             |${plan.treeString}
             |
             |$cause
           """.stripMargin)
    }

    checkAnswer(spark.sql(generatedSQL), Dataset.ofRows(spark, plan))
  }

  protected def checkSQL(df: DataFrame, expectedSQL: String): Unit = {
    checkSQL(df.queryExecution.analyzed, expectedSQL)
  }
}
