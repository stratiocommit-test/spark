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

import org.apache.spark.SparkFunSuite

class RuntimeConfigSuite extends SparkFunSuite {

  private def newConf(): RuntimeConfig = new RuntimeConfig

  test("set and get") {
    val conf = newConf()
    conf.set("k1", "v1")
    conf.set("k2", 2)
    conf.set("k3", value = false)

    assert(conf.get("k1") == "v1")
    assert(conf.get("k2") == "2")
    assert(conf.get("k3") == "false")

    intercept[NoSuchElementException] {
      conf.get("notset")
    }
  }

  test("getOption") {
    val conf = newConf()
    conf.set("k1", "v1")
    assert(conf.getOption("k1") == Some("v1"))
    assert(conf.getOption("notset") == None)
  }

  test("unset") {
    val conf = newConf()
    conf.set("k1", "v1")
    assert(conf.get("k1") == "v1")
    conf.unset("k1")
    intercept[NoSuchElementException] {
      conf.get("k1")
    }
  }
}
