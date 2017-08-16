/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming

import org.apache.spark.util.Utils

case class Duration (private val millis: Long) {

  def < (that: Duration): Boolean = (this.millis < that.millis)

  def <= (that: Duration): Boolean = (this.millis <= that.millis)

  def > (that: Duration): Boolean = (this.millis > that.millis)

  def >= (that: Duration): Boolean = (this.millis >= that.millis)

  def + (that: Duration): Duration = new Duration(millis + that.millis)

  def - (that: Duration): Duration = new Duration(millis - that.millis)

  def * (times: Int): Duration = new Duration(millis * times)

  def / (that: Duration): Double = millis.toDouble / that.millis.toDouble

  // Java-friendlier versions of the above.

  def less(that: Duration): Boolean = this < that

  def lessEq(that: Duration): Boolean = this <= that

  def greater(that: Duration): Boolean = this > that

  def greaterEq(that: Duration): Boolean = this >= that

  def plus(that: Duration): Duration = this + that

  def minus(that: Duration): Duration = this - that

  def times(times: Int): Duration = this * times

  def div(that: Duration): Double = this / that

  def isMultipleOf(that: Duration): Boolean =
    (this.millis % that.millis == 0)

  def min(that: Duration): Duration = if (this < that) this else that

  def max(that: Duration): Duration = if (this > that) this else that

  def isZero: Boolean = (this.millis == 0)

  override def toString: String = (millis.toString + " ms")

  def toFormattedString: String = millis.toString

  def milliseconds: Long = millis

  def prettyPrint: String = Utils.msDurationToString(millis)

}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of milliseconds.
 */
object Milliseconds {
  def apply(milliseconds: Long): Duration = new Duration(milliseconds)
}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of seconds.
 */
object Seconds {
  def apply(seconds: Long): Duration = new Duration(seconds * 1000)
}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of minutes.
 */
object Minutes {
  def apply(minutes: Long): Duration = new Duration(minutes * 60000)
}

// Java-friendlier versions of the objects above.
// Named "Durations" instead of "Duration" to avoid changing the case class's implied API.

object Durations {

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of milliseconds.
   */
  def milliseconds(milliseconds: Long): Duration = Milliseconds(milliseconds)

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of seconds.
   */
  def seconds(seconds: Long): Duration = Seconds(seconds)

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of minutes.
   */
  def minutes(minutes: Long): Duration = Minutes(minutes)

}
