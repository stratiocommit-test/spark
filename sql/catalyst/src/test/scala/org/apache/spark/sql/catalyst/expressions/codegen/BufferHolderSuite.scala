/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class BufferHolderSuite extends SparkFunSuite {

  test("SPARK-16071 Check the size limit to avoid integer overflow") {
    var e = intercept[UnsupportedOperationException] {
      new BufferHolder(new UnsafeRow(Int.MaxValue / 8))
    }
    assert(e.getMessage.contains("too many fields"))

    val holder = new BufferHolder(new UnsafeRow(1000))
    holder.reset()
    holder.grow(1000)
    e = intercept[UnsupportedOperationException] {
      holder.grow(Integer.MAX_VALUE)
    }
    assert(e.getMessage.contains("exceeds size limitation"))
  }
}
