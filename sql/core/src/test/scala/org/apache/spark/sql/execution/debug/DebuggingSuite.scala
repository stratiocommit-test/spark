/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.debug

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.TestData

class DebuggingSuite extends SparkFunSuite with SharedSQLContext {

  test("DataFrame.debug()") {
    testData.debug()
  }

  test("Dataset.debug()") {
    import testImplicits._
    testData.as[TestData].debug()
  }

  test("debugCodegen") {
    val res = codegenString(spark.range(10).groupBy("id").count().queryExecution.executedPlan)
    assert(res.contains("Subtree 1 / 2"))
    assert(res.contains("Subtree 2 / 2"))
    assert(res.contains("Object[]"))
  }
}
