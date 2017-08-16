/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.yarn

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.{IntParam, MemoryParam}

class ApplicationMasterArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var primaryPyFile: String = null
  var primaryRFile: String = null
  var userArgs: Seq[String] = Nil
  var propertiesFile: String = null

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()

    var args = inputArgs

    while (!args.isEmpty) {
      // --num-workers, --worker-memory, and --worker-cores are deprecated since 1.0,
      // the properties with executor in their names are preferred.
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--primary-py-file") :: value :: tail =>
          primaryPyFile = value
          args = tail

        case ("--primary-r-file") :: value :: tail =>
          primaryRFile = value
          args = tail

        case ("--arg") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--properties-file") :: value :: tail =>
          propertiesFile = value
          args = tail

        case _ =>
          printUsageAndExit(1, args)
      }
    }

    if (primaryPyFile != null && primaryRFile != null) {
      // scalastyle:off println
      System.err.println("Cannot have primary-py-file and primary-r-file at the same time")
      // scalastyle:on println
      System.exit(-1)
    }

    userArgs = userArgsBuffer.toList
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    // scalastyle:off println
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println("""
      |Usage: org.apache.spark.deploy.yarn.ApplicationMaster [options]
      |Options:
      |  --jar JAR_PATH       Path to your application's JAR file
      |  --class CLASS_NAME   Name of your application's main class
      |  --primary-py-file    A main Python file
      |  --primary-r-file     A main R file
      |  --arg ARG            Argument to be passed to your application's main class.
      |                       Multiple invocations are possible, each will be passed in order.
      |  --properties-file FILE Path to a custom Spark properties file.
      """.stripMargin)
    // scalastyle:on println
    System.exit(exitCode)
  }
}

object ApplicationMasterArguments {
  val DEFAULT_NUMBER_EXECUTORS = 2
}
