/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.test

import java.io.{InputStream, IOException}

import scala.sys.process.BasicIO

object ProcessTestUtils {
  class ProcessOutputCapturer(stream: InputStream, capture: String => Unit) extends Thread {
    this.setDaemon(true)

    override def run(): Unit = {
      try {
        BasicIO.processFully(capture)(stream)
      } catch { case _: IOException =>
        // Ignores the IOException thrown when the process termination, which closes the input
        // stream abruptly.
      }
    }
  }
}
