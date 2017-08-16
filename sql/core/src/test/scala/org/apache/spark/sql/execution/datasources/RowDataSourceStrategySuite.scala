/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources

import java.sql.DriverManager
import java.util.Properties

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class RowDataSourceStrategySuite extends SparkFunSuite with BeforeAndAfter with SharedSQLContext {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  before {
    Utils.classForName("org.h2.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement("create table test.inttypes (a INT, b INT, c INT)").executeUpdate()
    conn.prepareStatement("insert into test.inttypes values (1, 2, 3)").executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE TEMPORARY TABLE inttypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.INTTYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn.close()
  }

  test("SPARK-17673: Exchange reuse respects differences in output schema") {
    val df = sql("SELECT * FROM inttypes")
    val df1 = df.groupBy("a").agg("b" -> "min")
    val df2 = df.groupBy("a").agg("c" -> "min")
    val res = df1.union(df2)
    assert(res.distinct().count() == 2)  // would be 1 if the exchange was incorrectly reused
  }
}
