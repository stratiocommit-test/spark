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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.hive.test.TestHive

/**
 * A set of tests that validates support for Hive SerDe.
 */
class HiveSerDeSuite extends HiveComparisonTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    import TestHive._
    import org.apache.hadoop.hive.serde2.RegexSerDe
    super.beforeAll()
    TestHive.setCacheTables(false)
    sql(s"""CREATE TABLE IF NOT EXISTS sales (key STRING, value INT)
       |ROW FORMAT SERDE '${classOf[RegexSerDe].getCanonicalName}'
       |WITH SERDEPROPERTIES ("input.regex" = "([^ ]*)\t([^ ]*)")
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/sales.txt")}' INTO TABLE sales")
  }

  // table sales is not a cache table, and will be clear after reset
  createQueryTest("Read with RegexSerDe", "SELECT * FROM sales", false)

  createQueryTest(
    "Read and write with LazySimpleSerDe (tab separated)",
    "SELECT * from serdeins")

  createQueryTest("Read with AvroSerDe", "SELECT * FROM episodes")

  createQueryTest("Read Partitioned with AvroSerDe", "SELECT * FROM episodes_part")
}
