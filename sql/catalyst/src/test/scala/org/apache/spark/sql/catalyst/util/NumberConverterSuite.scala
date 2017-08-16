/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.NumberConverter.convert
import org.apache.spark.unsafe.types.UTF8String

class NumberConverterSuite extends SparkFunSuite {

  private[this] def checkConv(n: String, fromBase: Int, toBase: Int, expected: String): Unit = {
    assert(convert(UTF8String.fromString(n).getBytes, fromBase, toBase) ===
      UTF8String.fromString(expected))
  }

  test("convert") {
    checkConv("3", 10, 2, "11")
    checkConv("-15", 10, -16, "-F")
    checkConv("-15", 10, 16, "FFFFFFFFFFFFFFF1")
    checkConv("big", 36, 16, "3A48")
    checkConv("9223372036854775807", 36, 16, "FFFFFFFFFFFFFFFF")
    checkConv("11abc", 10, 16, "B")
  }

}
