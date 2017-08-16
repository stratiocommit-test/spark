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

import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}

class BlockReplicationPolicySuite extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  // Implicitly convert strings to BlockIds for test clarity.
  private implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)

  /**
   * Test if we get the required number of peers when using random sampling from
   * RandomBlockReplicationPolicy
   */
  test(s"block replication - random block replication policy") {
    val numBlockManagers = 10
    val storeSize = 1000
    val blockManagers = (1 to numBlockManagers).map { i =>
      BlockManagerId(s"store-$i", "localhost", 1000 + i, None)
    }
    val candidateBlockManager = BlockManagerId("test-store", "localhost", 1000, None)
    val replicationPolicy = new RandomBlockReplicationPolicy
    val blockId = "test-block"

    (1 to 10).foreach {numReplicas =>
      logDebug(s"Num replicas : $numReplicas")
      val randomPeers = replicationPolicy.prioritize(
        candidateBlockManager,
        blockManagers,
        mutable.HashSet.empty[BlockManagerId],
        blockId,
        numReplicas
      )
      logDebug(s"Random peers : ${randomPeers.mkString(", ")}")
      assert(randomPeers.toSet.size === numReplicas)

      // choosing n peers out of n
      val secondPass = replicationPolicy.prioritize(
        candidateBlockManager,
        randomPeers,
        mutable.HashSet.empty[BlockManagerId],
        blockId,
        numReplicas
      )
      logDebug(s"Random peers : ${secondPass.mkString(", ")}")
      assert(secondPass.toSet.size === numReplicas)
    }

  }

}
