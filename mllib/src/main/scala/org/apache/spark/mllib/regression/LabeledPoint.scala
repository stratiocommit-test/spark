/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.{LabeledPoint => NewLabeledPoint}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.NumericParser
import org.apache.spark.SparkException

/**
 * Class that represents the features and labels of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@Since("0.8.0")
@BeanInfo
case class LabeledPoint @Since("1.0.0") (
    @Since("0.8.0") label: Double,
    @Since("1.0.0") features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }

  private[spark] def asML: NewLabeledPoint = {
    NewLabeledPoint(label, features.asML)
  }
}

/**
 * Parser for [[org.apache.spark.mllib.regression.LabeledPoint]].
 *
 */
@Since("1.1.0")
object LabeledPoint {
  /**
   * Parses a string resulted from `LabeledPoint#toString` into
   * an [[org.apache.spark.mllib.regression.LabeledPoint]].
   *
   */
  @Since("1.1.0")
  def parse(s: String): LabeledPoint = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any) =>
          LabeledPoint(label, Vectors.parseNumeric(numeric))
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    } else { // dense format used before v1.0
      val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      LabeledPoint(label, features)
    }
  }

  private[spark] def fromML(point: NewLabeledPoint): LabeledPoint = {
    LabeledPoint(point.label, Vectors.fromML(point.features))
  }
}
