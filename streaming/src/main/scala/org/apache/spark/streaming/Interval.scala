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

private[streaming]
class Interval(val beginTime: Time, val endTime: Time) {
  def this(beginMs: Long, endMs: Long) = this(new Time(beginMs), new Time(endMs))

  def duration(): Duration = endTime - beginTime

  def + (time: Duration): Interval = {
    new Interval(beginTime + time, endTime + time)
  }

  def - (time: Duration): Interval = {
    new Interval(beginTime - time, endTime - time)
  }

  def < (that: Interval): Boolean = {
    if (this.duration != that.duration) {
      throw new Exception("Comparing two intervals with different durations [" + this + ", "
        + that + "]")
    }
    this.endTime < that.endTime
  }

  def <= (that: Interval): Boolean = (this < that || this == that)

  def > (that: Interval): Boolean = !(this <= that)

  def >= (that: Interval): Boolean = !(this < that)

  override def toString: String = "[" + beginTime + ", " + endTime + "]"
}

private[streaming]
object Interval {
  def currentInterval(duration: Duration): Interval = {
    val time = new Time(System.currentTimeMillis)
    val intervalBegin = time.floor(duration)
    new Interval(intervalBegin, intervalBegin + duration)
  }
}


