/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster

import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging

/**
 * Test the integration with [[SchedulerExtensionServices]]
 */
class ExtensionServiceIntegrationSuite extends SparkFunSuite
  with LocalSparkContext with BeforeAndAfter
  with Logging {

  val applicationId = new StubApplicationId(0, 1111L)
  val attemptId = new StubApplicationAttemptId(applicationId, 1)

  /*
   * Setup phase creates the spark context
   */
  before {
    val sparkConf = new SparkConf()
    sparkConf.set(SCHEDULER_SERVICES, Seq(classOf[SimpleExtensionService].getName()))
    sparkConf.setMaster("local").setAppName("ExtensionServiceIntegrationSuite")
    sc = new SparkContext(sparkConf)
  }

  test("Instantiate") {
    val services = new SchedulerExtensionServices()
    assertResult(Nil, "non-nil service list") {
      services.getServices
    }
    services.start(SchedulerExtensionServiceBinding(sc, applicationId))
    services.stop()
  }

  test("Contains SimpleExtensionService Service") {
    val services = new SchedulerExtensionServices()
    try {
      services.start(SchedulerExtensionServiceBinding(sc, applicationId))
      val serviceList = services.getServices
      assert(serviceList.nonEmpty, "empty service list")
      val (service :: Nil) = serviceList
      val simpleService = service.asInstanceOf[SimpleExtensionService]
      assert(simpleService.started.get, "service not started")
      services.stop()
      assert(!simpleService.started.get, "service not stopped")
    } finally {
      services.stop()
    }
  }
}


