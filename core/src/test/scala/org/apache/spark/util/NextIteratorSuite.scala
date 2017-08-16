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

import java.util.NoSuchElementException

import scala.collection.mutable.Buffer

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class NextIteratorSuite extends SparkFunSuite with Matchers {
  test("one iteration") {
    val i = new StubIterator(Buffer(1))
    i.hasNext should be (true)
    i.next should be (1)
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("two iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.hasNext should be (true)
    i.next should be (1)
    i.hasNext should be (true)
    i.next should be (2)
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("empty iteration") {
    val i = new StubIterator(Buffer())
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("close is called once for empty iterations") {
    val i = new StubIterator(Buffer())
    i.hasNext should be (false)
    i.hasNext should be (false)
    i.closeCalled should be (1)
  }

  test("close is called once for non-empty iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.next should be (1)
    i.next should be (2)
    // close isn't called until we check for the next element
    i.closeCalled should be (0)
    i.hasNext should be (false)
    i.closeCalled should be (1)
    i.hasNext should be (false)
    i.closeCalled should be (1)
  }

  class StubIterator(ints: Buffer[Int])  extends NextIterator[Int] {
    var closeCalled = 0

    override def getNext(): Int = {
      if (ints.size == 0) {
        finished = true
        0
      } else {
        ints.remove(0)
      }
    }

    override def close() {
      closeCalled += 1
    }
  }
}
