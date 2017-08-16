/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import java.io.PrintStream

import scala.collection.immutable.IndexedSeq

/**
 * Util for getting some stats from a small sample of numeric values, with some handy
 * summary functions.
 *
 * Entirely in memory, not intended as a good way to compute stats over large data sets.
 *
 * Assumes you are giving it a non-empty set of data
 */
private[spark] class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  require(startIdx < endIdx)
  def this(data: Traversable[Double]) = this(data.toArray, 0, data.size)
  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  /**
   * Get the value of the distribution at the given probabilities.  Probabilities should be
   * given from 0 to 1
   * @param probabilities
   */
  def getQuantiles(probabilities: Traversable[Double] = defaultProbabilities)
      : IndexedSeq[Double] = {
    probabilities.toIndexedSeq.map { p: Double => data(closestIndex(p)) }
  }

  private def closestIndex(p: Double) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }

  def showQuantiles(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    getQuantiles(defaultProbabilities).foreach{q => out.print(q + "\t")}
    out.println
    // scalastyle:on println
  }

  def statCounter: StatCounter = StatCounter(data.slice(startIdx, endIdx))

  /**
   * print a summary of this distribution to the given PrintStream.
   * @param out
   */
  def summary(out: PrintStream = System.out) {
    // scalastyle:off println
    out.println(statCounter)
    showQuantiles(out)
    // scalastyle:on println
  }
}

private[spark] object Distribution {

  def apply(data: Traversable[Double]): Option[Distribution] = {
    if (data.size > 0) {
      Some(new Distribution(data))
    } else {
      None
    }
  }

  def showQuantiles(out: PrintStream = System.out, quantiles: Traversable[Double]) {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    quantiles.foreach{q => out.print(q + "\t")}
    out.println
    // scalastyle:on println
  }
}
