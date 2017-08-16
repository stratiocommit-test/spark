/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper

class CoGroupedIteratorSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("basic") {
    val leftInput = Seq(create_row(1, "a"), create_row(1, "b"), create_row(2, "c")).iterator
    val rightInput = Seq(create_row(1, 2L), create_row(2, 3L), create_row(3, 4L)).iterator
    val leftGrouped = GroupedIterator(leftInput, Seq('i.int.at(0)), Seq('i.int, 's.string))
    val rightGrouped = GroupedIterator(rightInput, Seq('i.int.at(0)), Seq('i.int, 'l.long))
    val cogrouped = new CoGroupedIterator(leftGrouped, rightGrouped, Seq('i.int))

    val result = cogrouped.map {
      case (key, leftData, rightData) =>
        assert(key.numFields == 1)
        (key.getInt(0), leftData.toSeq, rightData.toSeq)
    }.toSeq
    assert(result ==
      (1,
        Seq(create_row(1, "a"), create_row(1, "b")),
        Seq(create_row(1, 2L))) ::
      (2,
        Seq(create_row(2, "c")),
        Seq(create_row(2, 3L))) ::
      (3,
        Seq.empty,
        Seq(create_row(3, 4L))) ::
      Nil
    )
  }

  test("SPARK-11393: respect the fact that GroupedIterator.hasNext is not idempotent") {
    val leftInput = Seq(create_row(2, "a")).iterator
    val rightInput = Seq(create_row(1, 2L)).iterator
    val leftGrouped = GroupedIterator(leftInput, Seq('i.int.at(0)), Seq('i.int, 's.string))
    val rightGrouped = GroupedIterator(rightInput, Seq('i.int.at(0)), Seq('i.int, 'l.long))
    val cogrouped = new CoGroupedIterator(leftGrouped, rightGrouped, Seq('i.int))

    val result = cogrouped.map {
      case (key, leftData, rightData) =>
        assert(key.numFields == 1)
        (key.getInt(0), leftData.toSeq, rightData.toSeq)
    }.toSeq

    assert(result ==
      (1,
        Seq.empty,
        Seq(create_row(1, 2L))) ::
      (2,
        Seq(create_row(2, "a")),
        Seq.empty) ::
      Nil
    )
  }
}
