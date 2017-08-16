/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.evaluation.binary

/**
 * Trait for a binary confusion matrix.
 */
private[evaluation] trait BinaryConfusionMatrix {
  /** number of true positives */
  def numTruePositives: Long

  /** number of false positives */
  def numFalsePositives: Long

  /** number of false negatives */
  def numFalseNegatives: Long

  /** number of true negatives */
  def numTrueNegatives: Long

  /** number of positives */
  def numPositives: Long = numTruePositives + numFalseNegatives

  /** number of negatives */
  def numNegatives: Long = numFalsePositives + numTrueNegatives
}

/**
 * Implementation of [[org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix]].
 *
 * @param count label counter for labels with scores greater than or equal to the current score
 * @param totalCount label counter for all labels
 */
private[evaluation] case class BinaryConfusionMatrixImpl(
    count: BinaryLabelCounter,
    totalCount: BinaryLabelCounter) extends BinaryConfusionMatrix {

  /** number of true positives */
  override def numTruePositives: Long = count.numPositives

  /** number of false positives */
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives */
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives */
  override def numNegatives: Long = totalCount.numNegatives
}
