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
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType

/**
 * :: DeveloperApi ::
 * Split applied to a feature
 * @param feature feature index
 * @param threshold Threshold for continuous feature.
 *                  Split left if feature is less than or equal to threshold, else right.
 * @param featureType type of feature -- categorical or continuous
 * @param categories Split left if categorical feature value is in this set, else right.
 */
@Since("1.0.0")
@DeveloperApi
case class Split(
    @Since("1.0.0") feature: Int,
    @Since("1.0.0") threshold: Double,
    @Since("1.0.0") featureType: FeatureType,
    @Since("1.0.0") categories: List[Double]) {

  override def toString: String = {
    s"Feature = $feature, threshold = $threshold, featureType = $featureType, " +
      s"categories = $categories"
  }
}

/**
 * Split with minimum threshold for continuous features. Helps with the smallest bin creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous
 */
private[tree] class DummyLowSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MinValue, featureType, List())

/**
 * Split with maximum threshold for continuous features. Helps with the highest bin creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous
 */
private[tree] class DummyHighSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MaxValue, featureType, List())

/**
 * Split with no acceptable feature values for categorical features. Helps with the first bin
 * creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous
 */
private[tree] class DummyCategoricalSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MaxValue, featureType, List())
