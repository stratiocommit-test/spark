/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.storage

import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryMode.ON_HEAP
import org.apache.spark.storage.memory.{MemoryStore, PartiallyUnrolledIterator}

class PartiallyUnrolledIteratorSuite extends SparkFunSuite with MockitoSugar {
  test("join two iterators") {
    val unrollSize = 1000
    val unroll = (0 until unrollSize).iterator
    val restSize = 500
    val rest = (unrollSize until restSize + unrollSize).iterator

    val memoryStore = mock[MemoryStore]
    val joinIterator = new PartiallyUnrolledIterator(memoryStore, ON_HEAP, unrollSize, unroll, rest)

    // Firstly iterate over unrolling memory iterator
    (0 until unrollSize).foreach { value =>
      assert(joinIterator.hasNext)
      assert(joinIterator.hasNext)
      assert(joinIterator.next() == value)
    }

    joinIterator.hasNext
    joinIterator.hasNext
    verify(memoryStore, times(1))
      .releaseUnrollMemoryForThisTask(Matchers.eq(ON_HEAP), Matchers.eq(unrollSize.toLong))

    // Secondly, iterate over rest iterator
    (unrollSize until unrollSize + restSize).foreach { value =>
      assert(joinIterator.hasNext)
      assert(joinIterator.hasNext)
      assert(joinIterator.next() == value)
    }

    joinIterator.close()
    // MemoryMode.releaseUnrollMemoryForThisTask is called only once
    verifyNoMoreInteractions(memoryStore)
  }
}
