/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.storage

import org.apache.spark.SparkFunSuite

class BlockIdSuite extends SparkFunSuite {
  def assertSame(id1: BlockId, id2: BlockId) {
    assert(id1.name === id2.name)
    assert(id1.hashCode === id2.hashCode)
    assert(id1 === id2)
  }

  def assertDifferent(id1: BlockId, id2: BlockId) {
    assert(id1.name != id2.name)
    assert(id1.hashCode != id2.hashCode)
    assert(id1 != id2)
  }

  test("test-bad-deserialization") {
    try {
      // Try to deserialize an invalid block id.
      BlockId("myblock")
      fail()
    } catch {
      case e: IllegalStateException => // OK
      case _: Throwable => fail()
    }
  }

  test("rdd") {
    val id = RDDBlockId(1, 2)
    assertSame(id, RDDBlockId(1, 2))
    assertDifferent(id, RDDBlockId(1, 1))
    assert(id.name === "rdd_1_2")
    assert(id.asRDDId.get.rddId === 1)
    assert(id.asRDDId.get.splitIndex === 2)
    assert(id.isRDD)
    assertSame(id, BlockId(id.toString))
  }

  test("shuffle") {
    val id = ShuffleBlockId(1, 2, 3)
    assertSame(id, ShuffleBlockId(1, 2, 3))
    assertDifferent(id, ShuffleBlockId(3, 2, 3))
    assert(id.name === "shuffle_1_2_3")
    assert(id.asRDDId === None)
    assert(id.shuffleId === 1)
    assert(id.mapId === 2)
    assert(id.reduceId === 3)
    assert(id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }

  test("broadcast") {
    val id = BroadcastBlockId(42)
    assertSame(id, BroadcastBlockId(42))
    assertDifferent(id, BroadcastBlockId(123))
    assert(id.name === "broadcast_42")
    assert(id.asRDDId === None)
    assert(id.broadcastId === 42)
    assert(id.isBroadcast)
    assertSame(id, BlockId(id.toString))
  }

  test("taskresult") {
    val id = TaskResultBlockId(60)
    assertSame(id, TaskResultBlockId(60))
    assertDifferent(id, TaskResultBlockId(61))
    assert(id.name === "taskresult_60")
    assert(id.asRDDId === None)
    assert(id.taskId === 60)
    assert(!id.isRDD)
    assertSame(id, BlockId(id.toString))
  }

  test("stream") {
    val id = StreamBlockId(1, 100)
    assertSame(id, StreamBlockId(1, 100))
    assertDifferent(id, StreamBlockId(2, 101))
    assert(id.name === "input-1-100")
    assert(id.asRDDId === None)
    assert(id.streamId === 1)
    assert(id.uniqueId === 100)
    assert(!id.isBroadcast)
    assertSame(id, BlockId(id.toString))
  }

  test("test") {
    val id = TestBlockId("abc")
    assertSame(id, TestBlockId("abc"))
    assertDifferent(id, TestBlockId("ab"))
    assert(id.name === "test_abc")
    assert(id.asRDDId === None)
    assert(id.id === "abc")
    assert(!id.isShuffle)
    assertSame(id, BlockId(id.toString))
  }
}
