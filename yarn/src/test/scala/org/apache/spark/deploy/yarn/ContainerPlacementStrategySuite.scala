/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.SparkFunSuite

class ContainerPlacementStrategySuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  private val yarnAllocatorSuite = new YarnAllocatorSuite
  import yarnAllocatorSuite._

  def createContainerRequest(nodes: Array[String]): ContainerRequest =
    new ContainerRequest(containerResource, nodes, null, YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)

  override def beforeEach() {
    yarnAllocatorSuite.beforeEach()
  }

  override def afterEach() {
    yarnAllocatorSuite.afterEach()
  }

  test("allocate locality preferred containers with enough resource and no matched existed " +
    "containers") {
    // 1. All the locations of current containers cannot satisfy the new requirements
    // 2. Current requested container number can fully satisfy the pending tasks.

    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host3" -> 15, "host4" -> 15, "host5" -> 10),
        handler.allocatedHostToContainersMap, Seq.empty)

    assert(localities.map(_.nodes) === Array(
      Array("host3", "host4", "host5"),
      Array("host3", "host4", "host5"),
      Array("host3", "host4")))
  }

  test("allocate locality preferred containers with enough resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    // 2. Current requested container number can fully satisfy the pending tasks.

    val handler = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
        handler.allocatedHostToContainersMap, Seq.empty)

    assert(localities.map(_.nodes) ===
      Array(null, Array("host2", "host3"), Array("host2", "host3")))
  }

  test("allocate locality preferred containers with limited resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    // 2. Current requested container number cannot fully satisfy the pending tasks.

    val handler = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
        handler.allocatedHostToContainersMap, Seq.empty)

    assert(localities.map(_.nodes) === Array(Array("host2", "host3")))
  }

  test("allocate locality preferred containers with fully matched containers") {
    // Current containers' locations can fully satisfy the new requirements

    val handler = createAllocator(5)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2"),
      createContainer("host2"),
      createContainer("host3")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
        handler.allocatedHostToContainersMap, Seq.empty)

    assert(localities.map(_.nodes) === Array(null, null, null))
  }

  test("allocate containers with no locality preference") {
    // Request new container without locality preference

    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 0, Map.empty, handler.allocatedHostToContainersMap, Seq.empty)

    assert(localities.map(_.nodes) === Array(null))
  }

  test("allocate locality preferred containers by considering the localities of pending requests") {
    val handler = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val pendingAllocationRequests = Seq(
      createContainerRequest(Array("host2", "host3")),
      createContainerRequest(Array("host1", "host4")))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
        handler.allocatedHostToContainersMap, pendingAllocationRequests)

    assert(localities.map(_.nodes) === Array(Array("host3")))
  }
}
