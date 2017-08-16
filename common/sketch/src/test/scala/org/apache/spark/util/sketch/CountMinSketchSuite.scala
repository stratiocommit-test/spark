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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class CountMinSketchSuite extends FunSuite { // scalastyle:ignore funsuite
  private val epsOfTotalCount = 0.0001

  private val confidence = 0.99

  private val seed = 42

  // Serializes and deserializes a given `CountMinSketch`, then checks whether the deserialized
  // version is equivalent to the original one.
  private def checkSerDe(sketch: CountMinSketch): Unit = {
    val out = new ByteArrayOutputStream()
    sketch.writeTo(out)

    val in = new ByteArrayInputStream(out.toByteArray)
    val deserialized = CountMinSketch.readFrom(in)

    assert(sketch === deserialized)
  }

  def testAccuracy[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"accuracy - $typeName") {
      // Uses fixed seed to ensure reproducible test execution
      val r = new Random(31)

      val numAllItems = 1000000
      val allItems = Array.fill(numAllItems)(itemGenerator(r))

      val numSamples = numAllItems / 10
      val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))

      val exactFreq = {
        val sampledItems = sampledItemIndices.map(allItems)
        sampledItems.groupBy(identity).mapValues(_.length.toLong)
      }

      val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
      checkSerDe(sketch)

      sampledItemIndices.foreach(i => sketch.add(allItems(i)))
      checkSerDe(sketch)

      val probCorrect = {
        val numErrors = allItems.map { item =>
          val count = exactFreq.getOrElse(item, 0L)
          val ratio = (sketch.estimateCount(item) - count).toDouble / numAllItems
          if (ratio > epsOfTotalCount) 1 else 0
        }.sum

        1D - numErrors.toDouble / numAllItems
      }

      assert(
        probCorrect > confidence,
        s"Confidence not reached: required $confidence, reached $probCorrect"
      )
    }
  }

  def testMergeInPlace[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"mergeInPlace - $typeName") {
      // Uses fixed seed to ensure reproducible test execution
      val r = new Random(31)

      val numToMerge = 5
      val numItemsPerSketch = 100000
      val perSketchItems = Array.fill(numToMerge, numItemsPerSketch) {
        itemGenerator(r)
      }

      val sketches = perSketchItems.map { items =>
        val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
        checkSerDe(sketch)

        items.foreach(sketch.add)
        checkSerDe(sketch)

        sketch
      }

      val mergedSketch = sketches.reduce(_ mergeInPlace _)
      checkSerDe(mergedSketch)

      val expectedSketch = {
        val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
        perSketchItems.foreach(_.foreach(sketch.add))
        sketch
      }

      perSketchItems.foreach {
        _.foreach { item =>
          assert(mergedSketch.estimateCount(item) === expectedSketch.estimateCount(item))
        }
      }
    }
  }

  def testItemType[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    testAccuracy[T](typeName)(itemGenerator)
    testMergeInPlace[T](typeName)(itemGenerator)
  }

  testItemType[Byte]("Byte") { _.nextInt().toByte }

  testItemType[Short]("Short") { _.nextInt().toShort }

  testItemType[Int]("Int") { _.nextInt() }

  testItemType[Long]("Long") { _.nextLong() }

  testItemType[String]("String") { r => r.nextString(r.nextInt(20)) }

  test("incompatible merge") {
    intercept[IncompatibleMergeException] {
      CountMinSketch.create(10, 10, 1).mergeInPlace(null)
    }

    intercept[IncompatibleMergeException] {
      val sketch1 = CountMinSketch.create(10, 20, 1)
      val sketch2 = CountMinSketch.create(10, 20, 2)
      sketch1.mergeInPlace(sketch2)
    }

    intercept[IncompatibleMergeException] {
      val sketch1 = CountMinSketch.create(10, 10, 1)
      val sketch2 = CountMinSketch.create(10, 20, 2)
      sketch1.mergeInPlace(sketch2)
    }
  }
}
