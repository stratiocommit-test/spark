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

import scala.tools.nsc.{Settings, CompilerCommand}
import scala.Predef._
import org.apache.spark.annotation.DeveloperApi

/**
 * Command class enabling Spark-specific command line options (provided by
 * <i>org.apache.spark.repl.SparkRunnerSettings</i>).
 *
 * @example new SparkCommandLine(Nil).settings
 *
 * @param args The list of command line arguments
 * @param settings The underlying settings to associate with this set of
 *                 command-line options
 */
@DeveloperApi
class SparkCommandLine(args: List[String], override val settings: Settings)
    extends CompilerCommand(args, settings) {
  def this(args: List[String], error: String => Unit) {
    this(args, new SparkRunnerSettings(error))
  }

  def this(args: List[String]) {
    // scalastyle:off println
    this(args, str => Console.println("Error: " + str))
    // scalastyle:on println
  }
}
