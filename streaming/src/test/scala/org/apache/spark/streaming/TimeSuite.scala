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

class TimeSuite extends TestSuiteBase {

  test("less") {
    assert(new Time(999) < new Time(1000))
    assert(new Time(0) < new Time(1))
    assert(!(new Time(1000) < new Time(999)))
    assert(!(new Time(1000) < new Time(1000)))
  }

  test("lessEq") {
    assert(new Time(999) <= new Time(1000))
    assert(new Time(0) <= new Time(1))
    assert(!(new Time(1000) <= new Time(999)))
    assert(new Time(1000) <= new Time(1000))
  }

  test("greater") {
    assert(!(new Time(999) > new Time(1000)))
    assert(!(new Time(0) > new Time(1)))
    assert(new Time(1000) > new Time(999))
    assert(!(new Time(1000) > new Time(1000)))
  }

  test("greaterEq") {
    assert(!(new Time(999) >= new Time(1000)))
    assert(!(new Time(0) >= new Time(1)))
    assert(new Time(1000) >= new Time(999))
    assert(new Time(1000) >= new Time(1000))
  }

  test("plus") {
    assert((new Time(1000) + new Duration(100)) == new Time(1100))
    assert((new Time(1000) + new Duration(0)) == new Time(1000))
  }

  test("minus Time") {
    assert((new Time(1000) - new Time(100)) == new Duration(900))
    assert((new Time(1000) - new Time(0)) == new Duration(1000))
    assert((new Time(1000) - new Time(1000)) == new Duration(0))
  }

  test("minus Duration") {
    assert((new Time(1000) - new Duration(100)) == new Time(900))
    assert((new Time(1000) - new Duration(0)) == new Time(1000))
    assert((new Time(1000) - new Duration(1000)) == new Time(0))
  }

  test("floor") {
    assert(new Time(1350).floor(new Duration(200)) == new Time(1200))
    assert(new Time(1200).floor(new Duration(200)) == new Time(1200))
    assert(new Time(199).floor(new Duration(200)) == new Time(0))
    assert(new Time(1).floor(new Duration(1)) == new Time(1))
    assert(new Time(1350).floor(new Duration(200), new Time(50)) == new Time(1250))
    assert(new Time(1350).floor(new Duration(200), new Time(150)) == new Time(1350))
    assert(new Time(1350).floor(new Duration(200), new Time(200)) == new Time(1200))
  }

  test("isMultipleOf") {
    assert(new Time(1000).isMultipleOf(new Duration(5)))
    assert(new Time(1000).isMultipleOf(new Duration(1000)))
    assert(new Time(1000).isMultipleOf(new Duration(1)))
    assert(!new Time(1000).isMultipleOf(new Duration(6)))
  }

  test("min") {
    assert(new Time(999).min(new Time(1000)) == new Time(999))
    assert(new Time(1000).min(new Time(999)) == new Time(999))
    assert(new Time(1000).min(new Time(1000)) == new Time(1000))
  }

  test("max") {
    assert(new Time(999).max(new Time(1000)) == new Time(1000))
    assert(new Time(1000).max(new Time(999)) == new Time(1000))
    assert(new Time(1000).max(new Time(1000)) == new Time(1000))
  }

  test("until") {
    assert(new Time(1000).until(new Time(1100), new Duration(100)) ==
           Seq(Time(1000)))
    assert(new Time(1000).until(new Time(1000), new Duration(100)) ==
           Seq())
    assert(new Time(1000).until(new Time(1100), new Duration(30)) ==
           Seq(Time(1000), Time(1030), Time(1060), Time(1090)))
  }

  test("to") {
    assert(new Time(1000).to(new Time(1100), new Duration(100)) ==
           Seq(Time(1000), Time(1100)))
    assert(new Time(1000).to(new Time(1000), new Duration(100)) ==
           Seq(Time(1000)))
    assert(new Time(1000).to(new Time(1100), new Duration(30)) ==
           Seq(Time(1000), Time(1030), Time(1060), Time(1090)))
  }

}
