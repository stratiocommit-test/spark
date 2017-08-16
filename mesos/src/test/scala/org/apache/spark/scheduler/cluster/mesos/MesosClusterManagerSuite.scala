/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark._
import org.apache.spark.internal.config._

class MesosClusterManagerSuite extends SparkFunSuite with LocalSparkContext {
    def testURL(masterURL: String, expectedClass: Class[_], coarse: Boolean) {
      val conf = new SparkConf().set("spark.mesos.coarse", coarse.toString)
      sc = new SparkContext("local", "test", conf)
      val clusterManager = new MesosClusterManager()

      assert(clusterManager.canCreate(masterURL))
      val taskScheduler = clusterManager.createTaskScheduler(sc, masterURL)
      val sched = clusterManager.createSchedulerBackend(sc, masterURL, taskScheduler)
      assert(sched.getClass === expectedClass)
    }

    test("mesos fine-grained") {
      testURL("mesos://localhost:1234", classOf[MesosFineGrainedSchedulerBackend], coarse = false)
    }

    test("mesos coarse-grained") {
      testURL("mesos://localhost:1234", classOf[MesosCoarseGrainedSchedulerBackend], coarse = true)
    }

    test("mesos with zookeeper") {
      testURL("mesos://zk://localhost:1234,localhost:2345",
          classOf[MesosFineGrainedSchedulerBackend],
          coarse = false)
    }

    test("mesos with i/o encryption throws error") {
      val se = intercept[SparkException] {
        val conf = new SparkConf().setAppName("test").set(IO_ENCRYPTION_ENABLED, true)
        sc = new SparkContext("mesos", "test", conf)
      }
      assert(se.getCause().isInstanceOf[IllegalArgumentException])
    }
}
