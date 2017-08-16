/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.xml

import java.io.IOException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.xml.UDFXPathUtil.ReusableStringReader

/**
 * Unit tests for [[UDFXPathUtil.ReusableStringReader]].
 *
 * Loosely based on Hive's TestReusableStringReader.java.
 */
class ReusableStringReaderSuite extends SparkFunSuite {

  private val fox = "Quick brown fox jumps over the lazy dog."

  test("empty reader") {
    val reader = new ReusableStringReader

    intercept[IOException] {
      reader.read()
    }

    intercept[IOException] {
      reader.ready()
    }

    reader.close()
  }

  test("mark reset") {
    val reader = new ReusableStringReader

    if (reader.markSupported()) {
      reader.asInstanceOf[ReusableStringReader].set(fox)
      assert(reader.ready())

      val cc = new Array[Char](6)
      var read = reader.read(cc)
      assert(read == 6)
      assert("Quick " == new String(cc))

      reader.mark(100)

      read = reader.read(cc)
      assert(read == 6)
      assert("brown " == new String(cc))

      reader.reset()
      read = reader.read(cc)
      assert(read == 6)
      assert("brown " == new String(cc))
    }
    reader.close()
  }

  test("skip") {
    val reader = new ReusableStringReader
    reader.asInstanceOf[ReusableStringReader].set(fox)

    // skip entire the data:
    var skipped = reader.skip(fox.length() + 1)
    assert(fox.length() == skipped)
    assert(-1 == reader.read())

    reader.asInstanceOf[ReusableStringReader].set(fox) // reset the data
    val cc = new Array[Char](6)
    var read = reader.read(cc)
    assert(read == 6)
    assert("Quick " == new String(cc))

    // skip some piece of data:
    skipped = reader.skip(30)
    assert(skipped == 30)
    read = reader.read(cc)
    assert(read == 4)
    assert("dog." == new String(cc, 0, read))

    // skip when already at EOF:
    skipped = reader.skip(300)
    assert(skipped == 0, skipped)
    assert(reader.read() == -1)

    reader.close()
  }
}
