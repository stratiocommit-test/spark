/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming

class DurationSuite extends TestSuiteBase {

  test("less") {
    assert(new Duration(999) < new Duration(1000))
    assert(new Duration(0) < new Duration(1))
    assert(!(new Duration(1000) < new Duration(999)))
    assert(!(new Duration(1000) < new Duration(1000)))
  }

  test("lessEq") {
    assert(new Duration(999) <= new Duration(1000))
    assert(new Duration(0) <= new Duration(1))
    assert(!(new Duration(1000) <= new Duration(999)))
    assert(new Duration(1000) <= new Duration(1000))
  }

  test("greater") {
    assert(!(new Duration(999) > new Duration(1000)))
    assert(!(new Duration(0) > new Duration(1)))
    assert(new Duration(1000) > new Duration(999))
    assert(!(new Duration(1000) > new Duration(1000)))
  }

  test("greaterEq") {
    assert(!(new Duration(999) >= new Duration(1000)))
    assert(!(new Duration(0) >= new Duration(1)))
    assert(new Duration(1000) >= new Duration(999))
    assert(new Duration(1000) >= new Duration(1000))
  }

  test("plus") {
    assert((new Duration(1000) + new Duration(100)) == new Duration(1100))
    assert((new Duration(1000) + new Duration(0)) == new Duration(1000))
  }

  test("minus") {
    assert((new Duration(1000) - new Duration(100)) == new Duration(900))
    assert((new Duration(1000) - new Duration(0)) == new Duration(1000))
    assert((new Duration(1000) - new Duration(1000)) == new Duration(0))
  }

  test("times") {
    assert((new Duration(100) * 2) == new Duration(200))
    assert((new Duration(100) * 1) == new Duration(100))
    assert((new Duration(100) * 0) == new Duration(0))
  }

  test("div") {
    assert((new Duration(1000) / new Duration(5)) == 200.0)
    assert((new Duration(1000) / new Duration(1)) == 1000.0)
    assert((new Duration(1000) / new Duration(1000)) == 1.0)
    assert((new Duration(1000) / new Duration(2000)) == 0.5)
  }

  test("isMultipleOf") {
    assert(new Duration(1000).isMultipleOf(new Duration(5)))
    assert(new Duration(1000).isMultipleOf(new Duration(1000)))
    assert(new Duration(1000).isMultipleOf(new Duration(1)))
    assert(!new Duration(1000).isMultipleOf(new Duration(6)))
  }

  test("min") {
    assert(new Duration(999).min(new Duration(1000)) == new Duration(999))
    assert(new Duration(1000).min(new Duration(999)) == new Duration(999))
    assert(new Duration(1000).min(new Duration(1000)) == new Duration(1000))
  }

  test("max") {
    assert(new Duration(999).max(new Duration(1000)) == new Duration(1000))
    assert(new Duration(1000).max(new Duration(999)) == new Duration(1000))
    assert(new Duration(1000).max(new Duration(1000)) == new Duration(1000))
  }

  test("isZero") {
    assert(new Duration(0).isZero)
    assert(!(new Duration(1).isZero))
  }

  test("Milliseconds") {
    assert(new Duration(100) == Milliseconds(100))
  }

  test("Seconds") {
    assert(new Duration(30 * 1000) == Seconds(30))
  }

  test("Minutes") {
    assert(new Duration(2 * 60 * 1000) == Minutes(2))
  }

}
