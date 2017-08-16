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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class GroupedIteratorSuite extends SparkFunSuite {

  test("basic") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 1)
        key.getInt(0) -> data.map(encoder.fromRow).toSeq
    }.toSeq

    assert(result ==
      1 -> Seq(input(0), input(1)) ::
      2 -> Seq(input(2)) :: Nil)
  }

  test("group by 2 columns") {
    val schema = new StructType().add("i", IntegerType).add("l", LongType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()

    val input = Seq(
      Row(1, 2L, "a"),
      Row(1, 2L, "b"),
      Row(1, 3L, "c"),
      Row(2, 1L, "d"),
      Row(3, 2L, "e"))

    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0), 'l.long.at(1)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 2)
        (key.getInt(0), key.getLong(1), data.map(encoder.fromRow).toSeq)
    }.toSeq

    assert(result ==
      (1, 2L, Seq(input(0), input(1))) ::
      (1, 3L, Seq(input(2))) ::
      (2, 1L, Seq(input(3))) ::
      (3, 2L, Seq(input(4))) :: Nil)
  }

  test("do nothing to the value iterator") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0)), schema.toAttributes)

    assert(grouped.length == 2)
  }
}
