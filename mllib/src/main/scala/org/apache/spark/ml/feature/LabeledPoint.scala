/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vector

/**
 *
 * Class that represents the features and label of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@Since("2.0.0")
@BeanInfo
case class LabeledPoint(@Since("2.0.0") label: Double, @Since("2.0.0") features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
}
