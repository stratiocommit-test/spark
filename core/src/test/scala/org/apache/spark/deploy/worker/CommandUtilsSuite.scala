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

import org.scalatest.{Matchers, PrivateMethodTester}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils

class CommandUtilsSuite extends SparkFunSuite with Matchers with PrivateMethodTester {

  test("set libraryPath correctly") {
    val appId = "12345-worker321-9876"
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val cmd = new Command("mainClass", Seq(), Map(), Seq(), Seq("libraryPathToB"), Seq())
    val builder = CommandUtils.buildProcessBuilder(
      cmd, new SecurityManager(new SparkConf), 512, sparkHome, t => t)
    val libraryPath = Utils.libraryPathEnvName
    val env = builder.environment
    env.keySet should contain(libraryPath)
    assert(env.get(libraryPath).startsWith("libraryPathToB"))
  }

  test("auth secret shouldn't appear in java opts") {
    val buildLocalCommand = PrivateMethod[Command]('buildLocalCommand)
    val conf = new SparkConf
    val secret = "This is the secret sauce"
    // set auth secret
    conf.set(SecurityManager.SPARK_AUTH_SECRET_CONF, secret)
    val command = new Command("mainClass", Seq(), Map(), Seq(), Seq("lib"),
      Seq("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF + "=" + secret))

    // auth is not set
    var cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(!cmd.environment.contains(SecurityManager.ENV_AUTH_SECRET))

    // auth is set to false
    conf.set(SecurityManager.SPARK_AUTH_CONF, "false")
    cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(!cmd.environment.contains(SecurityManager.ENV_AUTH_SECRET))

    // auth is set to true
    conf.set(SecurityManager.SPARK_AUTH_CONF, "true")
    cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(cmd.environment(SecurityManager.ENV_AUTH_SECRET) === secret)
  }
}
