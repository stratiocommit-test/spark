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

import org.apache.spark.internal.Logging

object Main extends Logging {

  initializeLogIfNecessary(true)

  private var _interp: SparkILoop = _

  def interp = _interp

  def interp_=(i: SparkILoop) { _interp = i }

  def main(args: Array[String]) {
    _interp = new SparkILoop
    _interp.process(args)
  }
}
