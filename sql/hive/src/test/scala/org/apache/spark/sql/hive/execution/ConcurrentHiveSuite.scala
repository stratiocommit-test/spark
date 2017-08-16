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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.hive.test.TestHiveContext

class ConcurrentHiveSuite extends SparkFunSuite with BeforeAndAfterAll {
  ignore("multiple instances not supported") {
    test("Multiple Hive Instances") {
      (1 to 10).map { i =>
        val conf = new SparkConf()
        conf.set("spark.ui.enabled", "false")
        val ts =
          new TestHiveContext(new SparkContext("local", s"TestSQLContext$i", conf))
        ts.sparkSession.sql("SHOW TABLES").collect()
        ts.sparkSession.sql("SELECT * FROM src").collect()
        ts.sparkSession.sql("SHOW TABLES").collect()
      }
    }
  }
}
