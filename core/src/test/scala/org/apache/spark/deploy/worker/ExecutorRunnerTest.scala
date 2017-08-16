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

import java.io.File

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.{ApplicationDescription, Command, ExecutorState}

class ExecutorRunnerTest extends SparkFunSuite {
  test("command includes appId") {
    val appId = "12345-worker321-9876"
    val conf = new SparkConf
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val appDesc = new ApplicationDescription("app name", Some(8), 500,
      Command("foo", Seq(appId), Map(), Seq(), Seq(), Seq()), "appUiUrl")
    val er = new ExecutorRunner(appId, 1, appDesc, 8, 500, null, "blah", "worker321", 123,
      "publicAddr", new File(sparkHome), new File("ooga"), "blah", conf, Seq("localDir"),
      ExecutorState.RUNNING)
    val builder = CommandUtils.buildProcessBuilder(
      appDesc.command, new SecurityManager(conf), 512, sparkHome, er.substituteVariables)
    val builderCommand = builder.command()
    assert(builderCommand.get(builderCommand.size() - 1) === appId)
  }
}
