/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import org.apache.spark._

class AccumulatorV2Suite extends SparkFunSuite {

  test("LongAccumulator add/avg/sum/count/isZero") {
    val acc = new LongAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.avg.isNaN)

    acc.add(0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0)
    assert(acc.avg == 0.0)

    acc.add(1)
    assert(acc.count == 2)
    assert(acc.sum == 1)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(new java.lang.Long(2))
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new LongAccumulator
    acc2.add(2)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.avg == 1.25)
  }

  test("DoubleAccumulator add/avg/sum/count/isZero") {
    val acc = new DoubleAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.avg.isNaN)

    acc.add(0.0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0.0)
    assert(acc.avg == 0.0)

    acc.add(1.0)
    assert(acc.count == 2)
    assert(acc.sum == 1.0)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(new java.lang.Double(2.0))
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new DoubleAccumulator
    acc2.add(2.0)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.avg == 1.25)
  }

  test("ListAccumulator") {
    val acc = new CollectionAccumulator[Double]
    assert(acc.value.isEmpty)
    assert(acc.isZero)

    acc.add(0.0)
    assert(acc.value.contains(0.0))
    assert(!acc.isZero)

    acc.add(new java.lang.Double(1.0))

    val acc2 = acc.copyAndReset()
    assert(acc2.value.isEmpty)
    assert(acc2.isZero)

    assert(acc.value.contains(1.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 2)

    acc2.add(2.0)
    assert(acc2.value.contains(2.0))
    assert(!acc2.isZero)
    assert(acc2.value.size() === 1)

    // Test merging
    acc.merge(acc2)
    assert(acc.value.contains(2.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 3)

    val acc3 = acc.copy()
    assert(acc3.value.contains(2.0))
    assert(!acc3.isZero)
    assert(acc3.value.size() === 3)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value.isEmpty)
  }

  test("LegacyAccumulatorWrapper") {
    val acc = new LegacyAccumulatorWrapper("default", AccumulatorParam.StringAccumulatorParam)
    assert(acc.value === "default")
    assert(!acc.isZero)

    acc.add("foo")
    assert(acc.value === "foo")
    assert(!acc.isZero)

    acc.add(new java.lang.String("bar"))

    val acc2 = acc.copyAndReset()
    assert(acc2.value === "")
    assert(acc2.isZero)

    assert(acc.value === "bar")
    assert(!acc.isZero)

    acc2.add("baz")
    assert(acc2.value === "baz")
    assert(!acc2.isZero)

    // Test merging
    acc.merge(acc2)
    assert(acc.value === "baz")
    assert(!acc.isZero)

    val acc3 = acc.copy()
    assert(acc3.value === "baz")
    assert(!acc3.isZero)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value === "")
  }
}
