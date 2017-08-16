/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset}

trait OffsetSuite extends SparkFunSuite {
  /** Creates test to check all the comparisons of offsets given a `one` that is less than `two`. */
  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparison $one <=> $two") {
      assert(one == one)
      assert(two == two)
      assert(one != two)
      assert(two != one)
    }
  }
}

class LongOffsetSuite extends OffsetSuite {
  val one = LongOffset(1)
  val two = LongOffset(2)
  val three = LongOffset(3)
  compare(one, two)

  compare(LongOffset(SerializedOffset(one.json)),
          LongOffset(SerializedOffset(three.json)))
}


