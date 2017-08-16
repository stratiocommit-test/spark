/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.thriftserver

import java.sql.DriverManager

import org.apache.hive.jdbc.HiveDriver

import org.apache.spark.util.Utils

class JdbcConnectionUriSuite extends HiveThriftServer2Test {
  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  override def mode: ServerMode.Value = ServerMode.binary

  val JDBC_TEST_DATABASE = "jdbc_test_database"
  val USER = System.getProperty("user.name")
  val PASSWORD = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    statement.execute(s"CREATE DATABASE $JDBC_TEST_DATABASE")
    connection.close()
  }

  override protected def afterAll(): Unit = {
    try {
      val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
      val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
      val statement = connection.createStatement()
      statement.execute(s"DROP DATABASE $JDBC_TEST_DATABASE")
      connection.close()
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-17819 Support default database in connection URIs") {
    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/$JDBC_TEST_DATABASE"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    try {
      val resultSet = statement.executeQuery("select current_database()")
      resultSet.next()
      assert(resultSet.getString(1) === JDBC_TEST_DATABASE)
    } finally {
      statement.close()
      connection.close()
    }
  }
}
