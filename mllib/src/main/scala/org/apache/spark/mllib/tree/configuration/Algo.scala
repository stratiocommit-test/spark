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
 * Enum to select the algorithm for the decision tree
 */
@Since("1.0.0")
object Algo extends Enumeration {
  @Since("1.0.0")
  type Algo = Value
  @Since("1.0.0")
  val Classification, Regression = Value

  private[mllib] def fromString(name: String): Algo = name match {
    case "classification" | "Classification" => Classification
    case "regression" | "Regression" => Regression
    case _ => throw new IllegalArgumentException(s"Did not recognize Algo name: $name")
  }
}
