/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.collection

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.SizeEstimator

class PrimitiveVectorSuite extends SparkFunSuite {

  test("primitive value") {
    val vector = new PrimitiveVector[Int]

    for (i <- 0 until 1000) {
      vector += i
      assert(vector(i) === i)
    }

    assert(vector.size === 1000)
    assert(vector.size == vector.length)
    intercept[IllegalArgumentException] {
      vector(1000)
    }

    for (i <- 0 until 1000) {
      assert(vector(i) == i)
    }
  }

  test("non-primitive value") {
    val vector = new PrimitiveVector[String]

    for (i <- 0 until 1000) {
      vector += i.toString
      assert(vector(i) === i.toString)
    }

    assert(vector.size === 1000)
    assert(vector.size == vector.length)
    intercept[IllegalArgumentException] {
      vector(1000)
    }

    for (i <- 0 until 1000) {
      assert(vector(i) == i.toString)
    }
  }

  test("ideal growth") {
    val vector = new PrimitiveVector[Long](initialSize = 1)
    vector += 1
    for (i <- 1 until 1024) {
      vector += i
      assert(vector.size === i + 1)
      assert(vector.capacity === Integer.highestOneBit(i) * 2)
    }
    assert(vector.capacity === 1024)
    vector += 1024
    assert(vector.capacity === 2048)
  }

  test("ideal size") {
    val vector = new PrimitiveVector[Long](8192)
    for (i <- 0 until 8192) {
      vector += i
    }
    assert(vector.size === 8192)
    assert(vector.capacity === 8192)
    val actualSize = SizeEstimator.estimate(vector)
    val expectedSize = 8192 * 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    // Due to specialization wonkiness, we need to ensure we don't have 2 copies of the array.
    assert(actualSize < expectedSize * 1.1)
  }

  test("resizing") {
    val vector = new PrimitiveVector[Long]
    for (i <- 0 until 4097) {
      vector += i
    }
    assert(vector.size === 4097)
    assert(vector.capacity === 8192)
    vector.trim()
    assert(vector.size === 4097)
    assert(vector.capacity === 4097)
    vector.resize(5000)
    assert(vector.size === 4097)
    assert(vector.capacity === 5000)
    vector.resize(4000)
    assert(vector.size === 4000)
    assert(vector.capacity === 4000)
    vector.resize(5000)
    assert(vector.size === 4000)
    assert(vector.capacity === 5000)
    for (i <- 0 until 4000) {
      assert(vector(i) == i)
    }
    intercept[IllegalArgumentException] {
      vector(4000)
    }
  }
}
