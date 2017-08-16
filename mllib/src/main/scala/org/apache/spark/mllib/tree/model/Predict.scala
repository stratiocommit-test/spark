/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree.model

import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * :: DeveloperApi ::
 * Predicted value for a node
 * @param predict predicted value
 * @param prob probability of the label (classification only)
 */
@Since("1.2.0")
@DeveloperApi
class Predict @Since("1.2.0") (
    @Since("1.2.0") val predict: Double,
    @Since("1.2.0") val prob: Double = 0.0) extends Serializable {

  override def toString: String = s"$predict (prob = $prob)"

  override def equals(other: Any): Boolean = {
    other match {
      case p: Predict => predict == p.predict && prob == p.prob
      case _ => false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(predict: java.lang.Double, prob: java.lang.Double)
  }
}
