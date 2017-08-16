/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.python

import java.io.DataOutputStream
import java.net.Socket

import py4j.GatewayServer

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Process that starts a Py4J GatewayServer on an ephemeral port and communicates the bound port
 * back to its caller via a callback port specified by the caller.
 *
 * This process is launched (via SparkSubmit) by the PySpark driver (see java_gateway.py).
 */
private[spark] object PythonGatewayServer extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = Utils.tryOrExit {
    // Start a GatewayServer on an ephemeral port
    val gatewayServer: GatewayServer = new GatewayServer(null, 0)
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logDebug(s"Started PythonGatewayServer on port $boundPort")
    }

    // Communicate the bound port back to the caller via the caller-specified callback port
    val callbackHost = sys.env("_PYSPARK_DRIVER_CALLBACK_HOST")
    val callbackPort = sys.env("_PYSPARK_DRIVER_CALLBACK_PORT").toInt
    logDebug(s"Communicating GatewayServer port to Python driver at $callbackHost:$callbackPort")
    val callbackSocket = new Socket(callbackHost, callbackPort)
    val dos = new DataOutputStream(callbackSocket.getOutputStream)
    dos.writeInt(boundPort)
    dos.close()
    callbackSocket.close()

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}
