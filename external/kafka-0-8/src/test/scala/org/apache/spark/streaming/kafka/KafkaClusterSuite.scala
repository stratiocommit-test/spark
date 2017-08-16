/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kafka

import scala.util.Random

import kafka.common.TopicAndPartition
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite

class KafkaClusterSuite extends SparkFunSuite with BeforeAndAfterAll {
  private val topic = "kcsuitetopic" + Random.nextInt(10000)
  private val topicAndPartition = TopicAndPartition(topic, 0)
  private var kc: KafkaCluster = null

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll() {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, Map("a" -> 1))
    kc = new KafkaCluster(Map("metadata.broker.list" -> kafkaTestUtils.brokerAddress))
  }

  override def afterAll() {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("metadata apis") {
    val leader = kc.findLeaders(Set(topicAndPartition)).right.get(topicAndPartition)
    val leaderAddress = s"${leader._1}:${leader._2}"
    assert(leaderAddress === kafkaTestUtils.brokerAddress, "didn't get leader")

    val parts = kc.getPartitions(Set(topic)).right.get
    assert(parts(topicAndPartition), "didn't get partitions")

    val err = kc.getPartitions(Set(topic + "BAD"))
    assert(err.isLeft, "getPartitions for a nonexistant topic should be an error")
  }

  test("leader offset apis") {
    val earliest = kc.getEarliestLeaderOffsets(Set(topicAndPartition)).right.get
    assert(earliest(topicAndPartition).offset === 0, "didn't get earliest")

    val latest = kc.getLatestLeaderOffsets(Set(topicAndPartition)).right.get
    assert(latest(topicAndPartition).offset === 1, "didn't get latest")
  }

  test("consumer offset apis") {
    val group = "kcsuitegroup" + Random.nextInt(10000)

    val offset = Random.nextInt(10000)

    val set = kc.setConsumerOffsets(group, Map(topicAndPartition -> offset))
    assert(set.isRight, "didn't set consumer offsets")

    val get = kc.getConsumerOffsets(group, Set(topicAndPartition)).right.get
    assert(get(topicAndPartition) === offset, "didn't get consumer offsets")
  }
}
