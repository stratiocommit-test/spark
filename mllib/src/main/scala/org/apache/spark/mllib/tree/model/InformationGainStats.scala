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
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator

/**
 * :: DeveloperApi ::
 * Information gain statistics for each split
 * @param gain information gain value
 * @param impurity current node impurity
 * @param leftImpurity left node impurity
 * @param rightImpurity right node impurity
 * @param leftPredict left node predict
 * @param rightPredict right node predict
 */
@Since("1.0.0")
@DeveloperApi
class InformationGainStats(
    val gain: Double,
    val impurity: Double,
    val leftImpurity: Double,
    val rightImpurity: Double,
    val leftPredict: Predict,
    val rightPredict: Predict) extends Serializable {

  override def toString: String = {
    s"gain = $gain, impurity = $impurity, left impurity = $leftImpurity, " +
      s"right impurity = $rightImpurity"
  }

  override def equals(o: Any): Boolean = o match {
    case other: InformationGainStats =>
      gain == other.gain &&
      impurity == other.impurity &&
      leftImpurity == other.leftImpurity &&
      rightImpurity == other.rightImpurity &&
      leftPredict == other.leftPredict &&
      rightPredict == other.rightPredict

    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      gain: java.lang.Double,
      impurity: java.lang.Double,
      leftImpurity: java.lang.Double,
      rightImpurity: java.lang.Double,
      leftPredict,
      rightPredict)
  }
}

/**
 * Impurity statistics for each split
 * @param gain information gain value
 * @param impurity current node impurity
 * @param impurityCalculator impurity statistics for current node
 * @param leftImpurityCalculator impurity statistics for left child node
 * @param rightImpurityCalculator impurity statistics for right child node
 * @param valid whether the current split satisfies minimum info gain or
 *              minimum number of instances per node
 */
private[spark] class ImpurityStats(
    val gain: Double,
    val impurity: Double,
    val impurityCalculator: ImpurityCalculator,
    val leftImpurityCalculator: ImpurityCalculator,
    val rightImpurityCalculator: ImpurityCalculator,
    val valid: Boolean = true) extends Serializable {

  override def toString: String = {
    s"gain = $gain, impurity = $impurity, left impurity = $leftImpurity, " +
      s"right impurity = $rightImpurity"
  }

  def leftImpurity: Double = if (leftImpurityCalculator != null) {
    leftImpurityCalculator.calculate()
  } else {
    -1.0
  }

  def rightImpurity: Double = if (rightImpurityCalculator != null) {
    rightImpurityCalculator.calculate()
  } else {
    -1.0
  }
}

private[spark] object ImpurityStats {

  /**
   * Return an [[org.apache.spark.mllib.tree.model.ImpurityStats]] object to
   * denote that current split doesn't satisfies minimum info gain or
   * minimum number of instances per node.
   */
  def getInvalidImpurityStats(impurityCalculator: ImpurityCalculator): ImpurityStats = {
    new ImpurityStats(Double.MinValue, impurityCalculator.calculate(),
      impurityCalculator, null, null, false)
  }

  /**
   * Return an [[org.apache.spark.mllib.tree.model.ImpurityStats]] object
   * that only 'impurity' and 'impurityCalculator' are defined.
   */
  def getEmptyImpurityStats(impurityCalculator: ImpurityCalculator): ImpurityStats = {
    new ImpurityStats(Double.NaN, impurityCalculator.calculate(), impurityCalculator, null, null)
  }
}
