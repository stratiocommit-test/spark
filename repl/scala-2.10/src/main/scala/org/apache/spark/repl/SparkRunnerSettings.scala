/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.repl

import scala.tools.nsc.Settings

/**
 * <i>scala.tools.nsc.Settings</i> implementation adding Spark-specific REPL
 * command line options.
 */
private[repl] class SparkRunnerSettings(error: String => Unit) extends Settings(error) {
  val loadfiles = MultiStringSetting(
      "-i",
      "file",
      "load a file (assumes the code is given interactively)")
}
