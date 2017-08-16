/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.util

import java.util.TimeZone

/**
 * Helper functions for testing date and time functionality.
 */
object DateTimeTestUtils {

  val ALL_TIMEZONES: Seq[TimeZone] = TimeZone.getAvailableIDs.toSeq.map(TimeZone.getTimeZone)

  def withDefaultTimeZone[T](newDefaultTimeZone: TimeZone)(block: => T): T = {
    val originalDefaultTimeZone = TimeZone.getDefault
    try {
      DateTimeUtils.resetThreadLocals()
      TimeZone.setDefault(newDefaultTimeZone)
      block
    } finally {
      TimeZone.setDefault(originalDefaultTimeZone)
      DateTimeUtils.resetThreadLocals()
    }
  }
}
