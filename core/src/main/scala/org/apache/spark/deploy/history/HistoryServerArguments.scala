/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.history

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Command-line parser for the [[HistoryServer]].
 */
private[history] class HistoryServerArguments(conf: SparkConf, args: Array[String])
  extends Logging {
  private var propertiesFile: String = null

  parse(args.toList)

  @tailrec
  private def parse(args: List[String]): Unit = {
    if (args.length == 1) {
      setLogDirectory(args.head)
    } else {
      args match {
        case ("--dir" | "-d") :: value :: tail =>
          setLogDirectory(value)
          parse(tail)

        case ("--help" | "-h") :: tail =>
          printUsageAndExit(0)

        case ("--properties-file") :: value :: tail =>
          propertiesFile = value
          parse(tail)

        case Nil =>

        case _ =>
          printUsageAndExit(1)
      }
    }
  }

  private def setLogDirectory(value: String): Unit = {
    logWarning("Setting log directory through the command line is deprecated as of " +
      "Spark 1.1.0. Please set this through spark.history.fs.logDirectory instead.")
    conf.set("spark.history.fs.logDirectory", value)
  }

   // This mutates the SparkConf, so all accesses to it must be made after this line
   Utils.loadDefaultSparkProperties(conf, propertiesFile)

  private def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      """
      |Usage: HistoryServer [options]
      |
      |Options:
      |  DIR                         Deprecated; set spark.history.fs.logDirectory directly
      |  --dir DIR (-d DIR)          Deprecated; set spark.history.fs.logDirectory directly
      |  --properties-file FILE      Path to a custom Spark properties file.
      |                              Default is conf/spark-defaults.conf.
      |
      |Configuration options can be set by setting the corresponding JVM system property.
      |History Server options are always available; additional options depend on the provider.
      |
      |History Server options:
      |
      |  spark.history.ui.port              Port where server will listen for connections
      |                                     (default 18080)
      |  spark.history.acls.enable          Whether to enable view acls for all applications
      |                                     (default false)
      |  spark.history.provider             Name of history provider class (defaults to
      |                                     file system-based provider)
      |  spark.history.retainedApplications Max number of application UIs to keep loaded in memory
      |                                     (default 50)
      |FsHistoryProvider options:
      |
      |  spark.history.fs.logDirectory      Directory where app logs are stored
      |                                     (default: file:/tmp/spark-events)
      |  spark.history.fs.updateInterval    How often to reload log data from storage
      |                                     (in seconds, default: 10)
      |""".stripMargin)
    // scalastyle:on println
    System.exit(exitCode)
  }

}

