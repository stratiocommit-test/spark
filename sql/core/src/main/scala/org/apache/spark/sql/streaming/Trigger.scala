/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.Experimental
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * :: Experimental ::
 * Used to indicate how often results should be produced by a [[StreamingQuery]].
 *
 * @since 2.0.0
 */
@Experimental
sealed trait Trigger

/**
 * :: Experimental ::
 * A trigger that runs a query periodically based on the processing time. If `interval` is 0,
 * the query will run as fast as possible.
 *
 * Scala Example:
 * {{{
 *   df.write.trigger(ProcessingTime("10 seconds"))
 *
 *   import scala.concurrent.duration._
 *   df.write.trigger(ProcessingTime(10.seconds))
 * }}}
 *
 * Java Example:
 * {{{
 *   df.write.trigger(ProcessingTime.create("10 seconds"))
 *
 *   import java.util.concurrent.TimeUnit
 *   df.write.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
 * }}}
 *
 * @since 2.0.0
 */
@Experimental
case class ProcessingTime(intervalMs: Long) extends Trigger {
  require(intervalMs >= 0, "the interval of trigger should not be negative")
}

/**
 * :: Experimental ::
 * Used to create [[ProcessingTime]] triggers for [[StreamingQuery]]s.
 *
 * @since 2.0.0
 */
@Experimental
object ProcessingTime {

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   df.write.trigger(ProcessingTime("10 seconds"))
   * }}}
   *
   * @since 2.0.0
   */
  def apply(interval: String): ProcessingTime = {
    if (StringUtils.isBlank(interval)) {
      throw new IllegalArgumentException(
        "interval cannot be null or blank.")
    }
    val cal = if (interval.startsWith("interval")) {
      CalendarInterval.fromString(interval)
    } else {
      CalendarInterval.fromString("interval " + interval)
    }
    if (cal == null) {
      throw new IllegalArgumentException(s"Invalid interval: $interval")
    }
    if (cal.months > 0) {
      throw new IllegalArgumentException(s"Doesn't support month or year interval: $interval")
    }
    new ProcessingTime(cal.microseconds / 1000)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   import scala.concurrent.duration._
   *   df.write.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * @since 2.0.0
   */
  def apply(interval: Duration): ProcessingTime = {
    new ProcessingTime(interval.toMillis)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   df.write.trigger(ProcessingTime.create("10 seconds"))
   * }}}
   *
   * @since 2.0.0
   */
  def create(interval: String): ProcessingTime = {
    apply(interval)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   import java.util.concurrent.TimeUnit
   *   df.write.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.0.0
   */
  def create(interval: Long, unit: TimeUnit): ProcessingTime = {
    new ProcessingTime(unit.toMillis(interval))
  }
}
