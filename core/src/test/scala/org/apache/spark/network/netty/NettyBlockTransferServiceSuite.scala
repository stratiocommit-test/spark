/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.netty

import scala.util.Random

import org.mockito.Mockito.mock
import org.scalatest._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.network.BlockDataManager

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with ShouldMatchers {

  private var service0: NettyBlockTransferService = _
  private var service1: NettyBlockTransferService = _

  override def afterEach() {
    try {
      if (service0 != null) {
        service0.close()
        service0 = null
      }

      if (service1 != null) {
        service1.close()
        service1 = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("can bind to a random port") {
    service0 = createService(port = 0)
    service0.port should not be 0
  }

  test("can bind to two random ports") {
    service0 = createService(port = 0)
    service1 = createService(port = 0)
    service0.port should not be service1.port
  }

  test("can bind to a specific port") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
  }

  test("can bind to a specific port twice and the second increments") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
    service1 = createService(service0.port)
    // `service0.port` is occupied, so `service1.port` should not be `service0.port`
    verifyServicePort(expectedPort = service0.port + 1, actualPort = service1.port)
  }

  private def verifyServicePort(expectedPort: Int, actualPort: Int): Unit = {
    actualPort should be >= expectedPort
    // avoid testing equality in case of simultaneous tests
    actualPort should be <= (expectedPort + 10)
  }

  private def createService(port: Int): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(conf, securityManager, "localhost", "localhost",
      port, 1)
    service.init(blockDataManager)
    service
  }
}
