/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy

import java.net.URL

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.SparkConfWithEnv

class LogUrlsStandaloneSuite extends SparkFunSuite with LocalSparkContext {

  /** Length of time to wait while draining listener events. */
  private val WAIT_TIMEOUT_MILLIS = 10000

  test("verify that correct log urls get propagated from workers") {
    sc = new SparkContext("local-cluster[2,1,1024]", "test")

    val listener = new SaveExecutorInfo
    sc.addSparkListener(listener)

    // Trigger a job so that executors get added
    sc.parallelize(1 to 100, 4).map(_.toString).count()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.addedExecutorInfos.values.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
      // Browse to each URL to check that it's valid
      info.logUrlMap.foreach { case (logType, logUrl) =>
        val html = Source.fromURL(logUrl).mkString
        assert(html.contains(s"$logType log page"))
      }
    }
  }

  test("verify that log urls reflect SPARK_PUBLIC_DNS (SPARK-6175)") {
    val SPARK_PUBLIC_DNS = "public_dns"
    val conf = new SparkConfWithEnv(Map("SPARK_PUBLIC_DNS" -> SPARK_PUBLIC_DNS)).set(
      "spark.extraListeners", classOf[SaveExecutorInfo].getName)
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)

    // Trigger a job so that executors get added
    sc.parallelize(1 to 100, 4).map(_.toString).count()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val listeners = sc.listenerBus.findListenersByClass[SaveExecutorInfo]
    assert(listeners.size === 1)
    val listener = listeners(0)
    listener.addedExecutorInfos.values.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
      info.logUrlMap.values.foreach { logUrl =>
        assert(new URL(logUrl).getHost === SPARK_PUBLIC_DNS)
      }
    }
  }
}

private[spark] class SaveExecutorInfo extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }
}
