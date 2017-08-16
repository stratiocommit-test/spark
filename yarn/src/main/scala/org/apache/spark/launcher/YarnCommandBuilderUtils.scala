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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Properties

/**
 * Exposes methods from the launcher library that are used by the YARN backend.
 */
private[spark] object YarnCommandBuilderUtils {

  def quoteForBatchScript(arg: String): String = {
    CommandBuilderUtils.quoteForBatchScript(arg)
  }

  def findJarsDir(sparkHome: String): String = {
    val scalaVer = Properties.versionNumberString
      .split("\\.")
      .take(2)
      .mkString(".")
    CommandBuilderUtils.findJarsDir(sparkHome, scalaVer, true)
  }

  /**
   * Adds the perm gen configuration to the list of java options if needed and not yet added.
   *
   * Note that this method adds the option based on the local JVM version; if the node where
   * the container is running has a different Java version, there's a risk that the option will
   * not be added (e.g. if the AM is running Java 8 but the container's node is set up to use
   * Java 7).
   */
  def addPermGenSizeOpt(args: ListBuffer[String]): Unit = {
    CommandBuilderUtils.addPermGenSizeOpt(args.asJava)
  }

}
