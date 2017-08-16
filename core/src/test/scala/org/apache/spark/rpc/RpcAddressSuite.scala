/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.rpc

import org.apache.spark.{SparkException, SparkFunSuite}

class RpcAddressSuite extends SparkFunSuite {

  test("hostPort") {
    val address = RpcAddress("1.2.3.4", 1234)
    assert(address.host == "1.2.3.4")
    assert(address.port == 1234)
    assert(address.hostPort == "1.2.3.4:1234")
  }

  test("fromSparkURL") {
    val address = RpcAddress.fromSparkURL("spark://1.2.3.4:1234")
    assert(address.host == "1.2.3.4")
    assert(address.port == 1234)
  }

  test("fromSparkURL: a typo url") {
    val e = intercept[SparkException] {
      RpcAddress.fromSparkURL("spark://1.2. 3.4:1234")
    }
    assert("Invalid master URL: spark://1.2. 3.4:1234" === e.getMessage)
  }

  test("fromSparkURL: invalid scheme") {
    val e = intercept[SparkException] {
      RpcAddress.fromSparkURL("invalid://1.2.3.4:1234")
    }
    assert("Invalid master URL: invalid://1.2.3.4:1234" === e.getMessage)
  }

  test("toSparkURL") {
    val address = RpcAddress("1.2.3.4", 1234)
    assert(address.toSparkURL == "spark://1.2.3.4:1234")
  }
}
