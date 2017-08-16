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

import org.apache.spark.SparkFunSuite

class CompletionIteratorSuite extends SparkFunSuite {
  test("basic test") {
    var numTimesCompleted = 0
    val iter = List(1, 2, 3).iterator
    val completionIter = CompletionIterator[Int, Iterator[Int]](iter, { numTimesCompleted += 1 })

    assert(completionIter.hasNext)
    assert(completionIter.next() === 1)
    assert(numTimesCompleted === 0)

    assert(completionIter.hasNext)
    assert(completionIter.next() === 2)
    assert(numTimesCompleted === 0)

    assert(completionIter.hasNext)
    assert(completionIter.next() === 3)
    assert(numTimesCompleted === 0)

    assert(!completionIter.hasNext)
    assert(numTimesCompleted === 1)

    // SPARK-4264: Calling hasNext should not trigger the completion callback again.
    assert(!completionIter.hasNext)
    assert(numTimesCompleted === 1)
  }
}
