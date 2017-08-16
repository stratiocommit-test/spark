/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

/**
 * End-to-end tests for xpath expressions.
 */
class XPathFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("xpath_boolean") {
    val df = Seq("<a><b>b</b></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_boolean(xml, 'a/b')"), Row(true))
  }

  test("xpath_short, xpath_int, xpath_long") {
    val df = Seq("<a><b>1</b><b>2</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_short(xml, 'sum(a/b)')",
        "xpath_int(xml, 'sum(a/b)')",
        "xpath_long(xml, 'sum(a/b)')"),
      Row(3.toShort, 3, 3L))
  }

  test("xpath_float, xpath_double, xpath_number") {
    val df = Seq("<a><b>1.0</b><b>2.1</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_float(xml, 'sum(a/b)')",
        "xpath_double(xml, 'sum(a/b)')",
        "xpath_number(xml, 'sum(a/b)')"),
      Row(3.1.toFloat, 3.1, 3.1))
  }

  test("xpath_string") {
    val df = Seq("<a><b>b</b><c>cc</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_string(xml, 'a/c')"), Row("cc"))
  }

  test("xpath") {
    val df = Seq("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath(xml, 'a/*/text()')"), Row(Seq("b1", "b2", "b3", "c1", "c2")))
  }
}
