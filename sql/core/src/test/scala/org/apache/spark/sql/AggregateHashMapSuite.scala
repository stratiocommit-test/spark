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

import org.scalatest.BeforeAndAfter

class SingleLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {

  protected override def beforeAll(): Unit = {
    sparkConf.set("spark.sql.codegen.fallback", "false")
    sparkConf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
    super.beforeAll()
  }

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "false",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapSuite extends DataFrameAggregateSuite with BeforeAndAfter {

  protected override def beforeAll(): Unit = {
    sparkConf.set("spark.sql.codegen.fallback", "false")
    sparkConf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
    super.beforeAll()
  }

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "true",
      "configuration parameter changed in test body")
  }
}

class TwoLevelAggregateHashMapWithVectorizedMapSuite extends DataFrameAggregateSuite with
BeforeAndAfter {

  protected override def beforeAll(): Unit = {
    sparkConf.set("spark.sql.codegen.fallback", "false")
    sparkConf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
    sparkConf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
    super.beforeAll()
  }

  // adding some checking after each test is run, assuring that the configs are not changed
  // in test code
  after {
    assert(sparkConf.get("spark.sql.codegen.fallback") == "false",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.twolevel.enable") == "true",
      "configuration parameter changed in test body")
    assert(sparkConf.get("spark.sql.codegen.aggregate.map.vectorized.enable") == "true",
      "configuration parameter changed in test body")
  }
}

