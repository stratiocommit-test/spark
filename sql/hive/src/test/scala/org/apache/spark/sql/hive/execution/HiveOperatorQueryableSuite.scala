/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton

/**
 * A set of tests that validates commands can also be queried by like a table
 */
class HiveOperatorQueryableSuite extends QueryTest with TestHiveSingleton {
  import spark._

  test("SPARK-5324 query result of describe command") {
    hiveContext.loadTestTable("src")

    // Creates a temporary view with the output of a describe command
    sql("desc src").createOrReplaceTempView("mydesc")
    checkAnswer(
      sql("desc mydesc"),
      Seq(
        Row("col_name", "string", "name of the column"),
        Row("data_type", "string", "data type of the column"),
        Row("comment", "string", "comment of the column")))

    checkAnswer(
      sql("select * from mydesc"),
      Seq(
        Row("key", "int", null),
        Row("value", "string", null)))

    checkAnswer(
      sql("select col_name, data_type, comment from mydesc"),
      Seq(
        Row("key", "int", null),
        Row("value", "string", null)))
  }
}
