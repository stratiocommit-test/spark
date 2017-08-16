/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.sketch

import scala.util.Random

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class BitArraySuite extends FunSuite { // scalastyle:ignore funsuite

  test("error case when create BitArray") {
    intercept[IllegalArgumentException](new BitArray(0))
    intercept[IllegalArgumentException](new BitArray(64L * Integer.MAX_VALUE + 1))
  }

  test("bitSize") {
    assert(new BitArray(64).bitSize() == 64)
    // BitArray is word-aligned, so 65~128 bits need 2 long to store, which is 128 bits.
    assert(new BitArray(65).bitSize() == 128)
    assert(new BitArray(127).bitSize() == 128)
    assert(new BitArray(128).bitSize() == 128)
  }

  test("set") {
    val bitArray = new BitArray(64)
    assert(bitArray.set(1))
    // Only returns true if the bit changed.
    assert(!bitArray.set(1))
    assert(bitArray.set(2))
  }

  test("normal operation") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray = new BitArray(320)
    val indexes = (1 to 100).map(_ => r.nextInt(320).toLong).distinct

    indexes.foreach(bitArray.set)
    indexes.foreach(i => assert(bitArray.get(i)))
    assert(bitArray.cardinality() == indexes.length)
  }

  test("merge") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray1 = new BitArray(64 * 6)
    val bitArray2 = new BitArray(64 * 6)

    val indexes1 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct
    val indexes2 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct

    indexes1.foreach(bitArray1.set)
    indexes2.foreach(bitArray2.set)

    bitArray1.putAll(bitArray2)
    indexes1.foreach(i => assert(bitArray1.get(i)))
    indexes2.foreach(i => assert(bitArray1.get(i)))
    assert(bitArray1.cardinality() == (indexes1 ++ indexes2).distinct.length)
  }
}
