/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite

/**
 * Test various parser errors.
 */
class ErrorParserSuite extends SparkFunSuite {
  def intercept(sql: String, line: Int, startPosition: Int, messages: String*): Unit = {
    val e = intercept[ParseException](CatalystSqlParser.parsePlan(sql))

    // Check position.
    assert(e.line.isDefined)
    assert(e.line.get === line)
    assert(e.startPosition.isDefined)
    assert(e.startPosition.get === startPosition)

    // Check messages.
    val error = e.getMessage
    messages.foreach { message =>
      assert(error.contains(message))
    }
  }

  test("no viable input") {
    intercept("select ((r + 1) ", 1, 16, "no viable alternative at input", "----------------^^^")
  }

  test("extraneous input") {
    intercept("select 1 1", 1, 9, "extraneous input '1' expecting", "---------^^^")
    intercept("select *\nfrom r as q t", 2, 12, "extraneous input", "------------^^^")
  }

  test("mismatched input") {
    intercept("select * from r order by q from t", 1, 27,
      "mismatched input",
      "---------------------------^^^")
    intercept("select *\nfrom r\norder by q\nfrom t", 4, 0, "mismatched input", "^^^")
  }

  test("semantic errors") {
    intercept("select *\nfrom r\norder by q\ncluster by q", 3, 0,
      "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported",
      "^^^")
    intercept("select * from r except all select * from t", 1, 0,
      "EXCEPT ALL is not supported",
      "^^^")
  }
}
