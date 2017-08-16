/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.mesos

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}


private[mesos] class MesosClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 7077
  var name = "Spark Cluster"
  var webUiPort = 8081
  var masterUrl: String = _
  var zookeeperUrl: Option[String] = None
  var propertiesFile: String = _

  parse(args.toList)

  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--webui-port") :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--zk" | "-z") :: value :: tail =>
      zookeeperUrl = Some(value)
      parse(tail)

    case ("--master" | "-m") :: value :: tail =>
      if (!value.startsWith("mesos://")) {
        // scalastyle:off println
        System.err.println("Cluster dispatcher only supports mesos (uri begins with mesos://)")
        // scalastyle:on println
        System.exit(1)
      }
      masterUrl = value.stripPrefix("mesos://")
      parse(tail)

    case ("--name") :: value :: tail =>
      name = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil =>
      if (masterUrl == null) {
        // scalastyle:off println
        System.err.println("--master is required")
        // scalastyle:on println
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      "Usage: MesosClusterDispatcher [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST    Hostname to listen on\n" +
        "  -p PORT, --port PORT    Port to listen on (default: 7077)\n" +
        "  --webui-port WEBUI_PORT WebUI Port to listen on (default: 8081)\n" +
        "  --name NAME             Framework name to show in Mesos UI\n" +
        "  -m --master MASTER      URI for connecting to Mesos master\n" +
        "  -z --zk ZOOKEEPER       Comma delimited URLs for connecting to \n" +
        "                          Zookeeper for persistence\n" +
        "  --properties-file FILE  Path to a custom Spark properties file.\n" +
        "                          Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
