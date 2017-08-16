/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark._

class LauncherBackendSuite extends SparkFunSuite with Matchers {

  private val tests = Seq(
    "local" -> "local",
    "standalone/client" -> "local-cluster[1,1,1024]")

  tests.foreach { case (name, master) =>
    test(s"$name: launcher handle") {
      testWithMaster(master)
    }
  }

  private def testWithMaster(master: String): Unit = {
    val env = new java.util.HashMap[String, String]()
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1")
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setConf("spark.ui.enabled", "false")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, s"-Dtest.appender=console")
      .setMaster(master)
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(TestApp.getClass.getName().stripSuffix("$"))
      .startApplication()

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getAppId() should not be (null)
      }

      handle.stop()

      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

}

object TestApp {

  def main(args: Array[String]): Unit = {
    new SparkContext(new SparkConf()).parallelize(Seq(1)).foreach { i =>
      Thread.sleep(TimeUnit.SECONDS.toMillis(20))
    }
  }

}
