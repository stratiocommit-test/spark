/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.util

import org.apache.spark.{SparkException, SparkFunSuite}

class NumericParserSuite extends SparkFunSuite {

  test("parser") {
    val s = "((1.0,2e3),-4,[5e-6,7.0E8],+9)"
    val parsed = NumericParser.parse(s).asInstanceOf[Seq[_]]
    assert(parsed(0).asInstanceOf[Seq[_]] === Seq(1.0, 2.0e3))
    assert(parsed(1).asInstanceOf[Double] === -4.0)
    assert(parsed(2).asInstanceOf[Array[Double]] === Array(5.0e-6, 7.0e8))
    assert(parsed(3).asInstanceOf[Double] === 9.0)

    val malformatted = Seq("a", "[1,,]", "0.123.4", "1 2", "3+4")
    malformatted.foreach { s =>
      intercept[SparkException] {
        NumericParser.parse(s)
        throw new RuntimeException(s"Didn't detect malformatted string $s.")
      }
    }
  }

  test("parser with whitespaces") {
    val s = "(0.0, [1.0, 2.0])"
    val parsed = NumericParser.parse(s).asInstanceOf[Seq[_]]
    assert(parsed(0).asInstanceOf[Double] === 0.0)
    assert(parsed(1).asInstanceOf[Array[Double]] === Array(1.0, 2.0))
  }
}
