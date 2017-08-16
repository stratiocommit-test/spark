/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree.loss

import org.apache.spark.annotation.Since

@Since("1.2.0")
object Losses {

  @Since("1.2.0")
  def fromString(name: String): Loss = name match {
    case "leastSquaresError" => SquaredError
    case "leastAbsoluteError" => AbsoluteError
    case "logLoss" => LogLoss
    case _ => throw new IllegalArgumentException(s"Did not recognize Loss name: $name")
  }

}
