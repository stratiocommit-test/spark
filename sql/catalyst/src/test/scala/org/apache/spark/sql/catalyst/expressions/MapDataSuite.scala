/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import scala.collection._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataType, IntegerType, MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class MapDataSuite extends SparkFunSuite {

  test("inequality tests") {
    def u(str: String): UTF8String = UTF8String.fromString(str)

    // test data
    val testMap1 = Map(u("key1") -> 1)
    val testMap2 = Map(u("key1") -> 1, u("key2") -> 2)
    val testMap3 = Map(u("key1") -> 1)
    val testMap4 = Map(u("key1") -> 1, u("key2") -> 2)

    // ArrayBasedMapData
    val testArrayMap1 = ArrayBasedMapData(testMap1.toMap)
    val testArrayMap2 = ArrayBasedMapData(testMap2.toMap)
    val testArrayMap3 = ArrayBasedMapData(testMap3.toMap)
    val testArrayMap4 = ArrayBasedMapData(testMap4.toMap)
    assert(testArrayMap1 !== testArrayMap3)
    assert(testArrayMap2 !== testArrayMap4)

    // UnsafeMapData
    val unsafeConverter = UnsafeProjection.create(Array[DataType](MapType(StringType, IntegerType)))
    val row = new GenericInternalRow(1)
    def toUnsafeMap(map: ArrayBasedMapData): UnsafeMapData = {
      row.update(0, map)
      val unsafeRow = unsafeConverter.apply(row)
      unsafeRow.getMap(0).copy
    }
    assert(toUnsafeMap(testArrayMap1) !== toUnsafeMap(testArrayMap3))
    assert(toUnsafeMap(testArrayMap2) !== toUnsafeMap(testArrayMap4))
  }
}
