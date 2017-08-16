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

import java.io.File
import java.util.Date

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}

private[deploy] object DeployTestUtils {
  def createAppDesc(): ApplicationDescription = {
    val cmd = new Command("mainClass", List("arg1", "arg2"), Map(), Seq(), Seq(), Seq())
    new ApplicationDescription("name", Some(4), 1234, cmd, "appUiUrl")
  }

  def createAppInfo() : ApplicationInfo = {
    val appDesc = createAppDesc()
    val appInfo = new ApplicationInfo(JsonConstants.appInfoStartTime,
      "id", appDesc, JsonConstants.submitDate, null, Int.MaxValue)
    appInfo.endTime = JsonConstants.currTimeInMillis
    appInfo
  }

  def createDriverCommand(): Command = new Command(
    "org.apache.spark.FakeClass", Seq("some arg --and-some options -g foo"),
    Map(("K1", "V1"), ("K2", "V2")), Seq("cp1", "cp2"), Seq("lp1", "lp2"), Seq("-Dfoo")
  )

  def createDriverDesc(): DriverDescription =
    new DriverDescription("hdfs://some-dir/some.jar", 100, 3, false, createDriverCommand())

  def createDriverInfo(): DriverInfo = new DriverInfo(3, "driver-3",
    createDriverDesc(), new Date())

  def createWorkerInfo(): WorkerInfo = {
    val workerInfo = new WorkerInfo("id", "host", 8080, 4, 1234, null, "http://publicAddress:80")
    workerInfo.lastHeartbeat = JsonConstants.currTimeInMillis
    workerInfo
  }

  def createExecutorRunner(execId: Int): ExecutorRunner = {
    new ExecutorRunner(
      "appId",
      execId,
      createAppDesc(),
      4,
      1234,
      null,
      "workerId",
      "host",
      123,
      "publicAddress",
      new File("sparkHome"),
      new File("workDir"),
      "spark://worker",
      new SparkConf,
      Seq("localDir"),
      ExecutorState.RUNNING)
  }

  def createDriverRunner(driverId: String): DriverRunner = {
    val conf = new SparkConf()
    new DriverRunner(
      conf,
      driverId,
      new File("workDir"),
      new File("sparkHome"),
      createDriverDesc(),
      null,
      "spark://worker",
      new SecurityManager(conf))
  }
}
