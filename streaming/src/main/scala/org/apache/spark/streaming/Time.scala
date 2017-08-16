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

/**
 * This is a simple class that represents an absolute instant of time.
 * Internally, it represents time as the difference, measured in milliseconds, between the current
 * time and midnight, January 1, 1970 UTC. This is the same format as what is returned by
 * System.currentTimeMillis.
 */
case class Time(private val millis: Long) {

  def milliseconds: Long = millis

  def < (that: Time): Boolean = (this.millis < that.millis)

  def <= (that: Time): Boolean = (this.millis <= that.millis)

  def > (that: Time): Boolean = (this.millis > that.millis)

  def >= (that: Time): Boolean = (this.millis >= that.millis)

  def + (that: Duration): Time = new Time(millis + that.milliseconds)

  def - (that: Time): Duration = new Duration(millis - that.millis)

  def - (that: Duration): Time = new Time(millis - that.milliseconds)

  // Java-friendlier versions of the above.

  def less(that: Time): Boolean = this < that

  def lessEq(that: Time): Boolean = this <= that

  def greater(that: Time): Boolean = this > that

  def greaterEq(that: Time): Boolean = this >= that

  def plus(that: Duration): Time = this + that

  def minus(that: Time): Duration = this - that

  def minus(that: Duration): Time = this - that


  def floor(that: Duration): Time = {
    val t = that.milliseconds
    new Time((this.millis / t) * t)
  }

  def floor(that: Duration, zeroTime: Time): Time = {
    val t = that.milliseconds
    new Time(((this.millis - zeroTime.milliseconds) / t) * t + zeroTime.milliseconds)
  }

  def isMultipleOf(that: Duration): Boolean =
    (this.millis % that.milliseconds == 0)

  def min(that: Time): Time = if (this < that) this else that

  def max(that: Time): Time = if (this > that) this else that

  def until(that: Time, interval: Duration): Seq[Time] = {
    (this.milliseconds) until (that.milliseconds) by (interval.milliseconds) map (new Time(_))
  }

  def to(that: Time, interval: Duration): Seq[Time] = {
    (this.milliseconds) to (that.milliseconds) by (interval.milliseconds) map (new Time(_))
  }


  override def toString: String = (millis.toString + " ms")

}

object Time {
  implicit val ordering = Ordering.by((time: Time) => time.millis)
}
