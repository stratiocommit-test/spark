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

import org.scalatest.PrivateMethodTester

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend


class SparkContextSchedulerCreationSuite
  extends SparkFunSuite with LocalSparkContext with PrivateMethodTester with Logging {

  def createTaskScheduler(master: String): TaskSchedulerImpl =
    createTaskScheduler(master, "client")

  def createTaskScheduler(master: String, deployMode: String): TaskSchedulerImpl =
    createTaskScheduler(master, deployMode, new SparkConf())

  def createTaskScheduler(
      master: String,
      deployMode: String,
      conf: SparkConf): TaskSchedulerImpl = {
    // Create local SparkContext to setup a SparkEnv. We don't actually want to start() the
    // real schedulers, so we don't want to create a full SparkContext with the desired scheduler.
    sc = new SparkContext("local", "test", conf)
    val createTaskSchedulerMethod =
      PrivateMethod[Tuple2[SchedulerBackend, TaskScheduler]]('createTaskScheduler)
    val (_, sched) = SparkContext invokePrivate createTaskSchedulerMethod(sc, master, deployMode)
    sched.asInstanceOf[TaskSchedulerImpl]
  }

  test("bad-master") {
    val e = intercept[SparkException] {
      createTaskScheduler("localhost:1234")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local") {
    val sched = createTaskScheduler("local")
    sched.backend match {
      case s: LocalSchedulerBackend => assert(s.totalCores === 1)
      case _ => fail()
    }
  }

  test("local-*") {
    val sched = createTaskScheduler("local[*]")
    sched.backend match {
      case s: LocalSchedulerBackend =>
        assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n") {
    val sched = createTaskScheduler("local[5]")
    assert(sched.maxTaskFailures === 1)
    sched.backend match {
      case s: LocalSchedulerBackend => assert(s.totalCores === 5)
      case _ => fail()
    }
  }

  test("local-*-n-failures") {
    val sched = createTaskScheduler("local[* ,2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      case s: LocalSchedulerBackend =>
        assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n-failures") {
    val sched = createTaskScheduler("local[4, 2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      case s: LocalSchedulerBackend => assert(s.totalCores === 4)
      case _ => fail()
    }
  }

  test("bad-local-n") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("bad-local-n-failures") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*,4]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local-default-parallelism") {
    val conf = new SparkConf().set("spark.default.parallelism", "16")
    val sched = createTaskScheduler("local", "client", conf)

    sched.backend match {
      case s: LocalSchedulerBackend => assert(s.defaultParallelism() === 16)
      case _ => fail()
    }
  }

  test("local-cluster") {
    createTaskScheduler("local-cluster[3, 14, 1024]").backend match {
      case s: StandaloneSchedulerBackend => // OK
      case _ => fail()
    }
  }
}
