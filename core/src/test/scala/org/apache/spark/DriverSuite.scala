/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import java.io.File

import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._

import org.apache.spark.util.Utils

class DriverSuite extends SparkFunSuite with Timeouts {

  ignore("driver should exit after finishing without cleanup (SPARK-530)") {
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val masters = Table("master", "local", "local-cluster[2,1,1024]")
    forAll(masters) { (master: String) =>
      val process = Utils.executeCommand(
        Seq(s"$sparkHome/bin/spark-class", "org.apache.spark.DriverWithoutCleanup", master),
        new File(sparkHome),
        Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
      failAfter(60 seconds) { process.waitFor() }
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
  }
}

/**
 * Program that creates a Spark driver but doesn't call SparkContext#stop() or
 * sys.exit() after finishing.
 */
object DriverWithoutCleanup {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
    val conf = new SparkConf
    val sc = new SparkContext(args(0), "DriverWithoutCleanup", conf)
    sc.parallelize(1 to 100, 4).count()
  }
}
