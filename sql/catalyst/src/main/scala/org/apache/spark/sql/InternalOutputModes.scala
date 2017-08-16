/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import org.apache.spark.sql.streaming.OutputMode

/**
 * Internal helper class to generate objects representing various `OutputMode`s,
 */
private[sql] object InternalOutputModes {

  /**
   * OutputMode in which only the new rows in the streaming DataFrame/Dataset will be
   * written to the sink. This output mode can be only be used in queries that do not
   * contain any aggregation.
   */
  case object Append extends OutputMode

  /**
   * OutputMode in which all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time these is some updates. This output mode can only be used in queries
   * that contain aggregations.
   */
  case object Complete extends OutputMode

  /**
   * OutputMode in which only the rows in the streaming DataFrame/Dataset that were updated will be
   * written to the sink every time these is some updates. This output mode can only be used in
   * queries that contain aggregations.
   */
  case object Update extends OutputMode
}
