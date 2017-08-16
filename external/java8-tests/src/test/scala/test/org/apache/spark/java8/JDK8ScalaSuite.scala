/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package test.org.apache.spark.java8

import org.apache.spark.SharedSparkContext
import org.apache.spark.SparkFunSuite

/**
 * Test cases where JDK8-compiled Scala user code is used with Spark.
 */
class JDK8ScalaSuite extends SparkFunSuite with SharedSparkContext {
  test("basic RDD closure test (SPARK-6152)") {
    sc.parallelize(1 to 1000).map(x => x * x).count()
  }
}
