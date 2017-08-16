/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite

/**
 * Unit test suites for data source filters.
 */
class FiltersSuite extends SparkFunSuite {

  test("EqualTo references") {
    assert(EqualTo("a", "1").references.toSeq == Seq("a"))
    assert(EqualTo("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("EqualNullSafe references") {
    assert(EqualNullSafe("a", "1").references.toSeq == Seq("a"))
    assert(EqualNullSafe("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("GreaterThan references") {
    assert(GreaterThan("a", "1").references.toSeq == Seq("a"))
    assert(GreaterThan("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("GreaterThanOrEqual references") {
    assert(GreaterThanOrEqual("a", "1").references.toSeq == Seq("a"))
    assert(GreaterThanOrEqual("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("LessThan references") {
    assert(LessThan("a", "1").references.toSeq == Seq("a"))
    assert(LessThan("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("LessThanOrEqual references") {
    assert(LessThanOrEqual("a", "1").references.toSeq == Seq("a"))
    assert(LessThanOrEqual("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("In references") {
    assert(In("a", Array("1")).references.toSeq == Seq("a"))
    assert(In("a", Array("1", EqualTo("b", "2"))).references.toSeq == Seq("a", "b"))
  }

  test("IsNull references") {
    assert(IsNull("a").references.toSeq == Seq("a"))
  }

  test("IsNotNull references") {
    assert(IsNotNull("a").references.toSeq == Seq("a"))
  }

  test("And references") {
    assert(And(EqualTo("a", "1"), EqualTo("b", "1")).references.toSeq == Seq("a", "b"))
  }

  test("Or references") {
    assert(Or(EqualTo("a", "1"), EqualTo("b", "1")).references.toSeq == Seq("a", "b"))
  }

  test("StringStartsWith references") {
    assert(StringStartsWith("a", "str").references.toSeq == Seq("a"))
  }

  test("StringEndsWith references") {
    assert(StringEndsWith("a", "str").references.toSeq == Seq("a"))
  }

  test("StringContains references") {
    assert(StringContains("a", "str").references.toSeq == Seq("a"))
  }
}
