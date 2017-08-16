/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.worker

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.SparkConfWithEnv

class WorkerArgumentsTest extends SparkFunSuite {

  test("Memory can't be set to 0 when cmd line args leave off M or G") {
    val conf = new SparkConf
    val args = Array("-m", "10000", "spark://localhost:0000  ")
    intercept[IllegalStateException] {
      new WorkerArguments(args, conf)
    }
  }


  test("Memory can't be set to 0 when SPARK_WORKER_MEMORY env property leaves off M or G") {
    val args = Array("spark://localhost:0000  ")
    val conf = new SparkConfWithEnv(Map("SPARK_WORKER_MEMORY" -> "50000"))
    intercept[IllegalStateException] {
      new WorkerArguments(args, conf)
    }
  }

  test("Memory correctly set when SPARK_WORKER_MEMORY env property appends G") {
    val args = Array("spark://localhost:0000  ")
    val conf = new SparkConfWithEnv(Map("SPARK_WORKER_MEMORY" -> "5G"))
    val workerArgs = new WorkerArguments(args, conf)
    assert(workerArgs.memory === 5120)
  }

  test("Memory correctly set from args with M appended to memory value") {
    val conf = new SparkConf
    val args = Array("-m", "10000M", "spark://localhost:0000  ")

    val workerArgs = new WorkerArguments(args, conf)
    assert(workerArgs.memory === 10000)

  }

}
