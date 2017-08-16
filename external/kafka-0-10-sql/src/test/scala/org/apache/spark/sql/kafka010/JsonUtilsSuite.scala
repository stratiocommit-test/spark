/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite

class JsonUtilsSuite extends SparkFunSuite {

  test("parsing partitions") {
    val parsed = JsonUtils.partitions("""{"topicA":[0,1],"topicB":[4,6]}""")
    val expected = Array(
      new TopicPartition("topicA", 0),
      new TopicPartition("topicA", 1),
      new TopicPartition("topicB", 4),
      new TopicPartition("topicB", 6)
    )
    assert(parsed.toSeq === expected.toSeq)
  }

  test("parsing partitionOffsets") {
    val parsed = JsonUtils.partitionOffsets(
      """{"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}""")

    assert(parsed(new TopicPartition("topicA", 0)) === 23)
    assert(parsed(new TopicPartition("topicA", 1)) === -1)
    assert(parsed(new TopicPartition("topicB", 0)) === -2)
  }
}
