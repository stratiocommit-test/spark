/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree.configuration

import org.apache.spark.annotation.Since

/**
 * Enum for selecting the quantile calculation strategy
 */
@Since("1.0.0")
object QuantileStrategy extends Enumeration {
  @Since("1.0.0")
  type QuantileStrategy = Value
  @Since("1.0.0")
  val Sort, MinMax, ApproxHist = Value
}
