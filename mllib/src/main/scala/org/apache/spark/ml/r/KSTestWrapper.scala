/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.r

import org.apache.spark.mllib.stat.Statistics.kolmogorovSmirnovTest
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult
import org.apache.spark.sql.{DataFrame, Row}

private[r] class KSTestWrapper private (
    val testResult: KolmogorovSmirnovTestResult,
    val distName: String,
    val distParams: Array[Double]) {

  lazy val pValue = testResult.pValue

  lazy val statistic = testResult.statistic

  lazy val nullHypothesis = testResult.nullHypothesis

  lazy val degreesOfFreedom = testResult.degreesOfFreedom

  def summary: String = testResult.toString
}

private[r] object KSTestWrapper {

  def test(
      data: DataFrame,
      featureName: String,
      distName: String,
      distParams: Array[Double]): KSTestWrapper = {

    val rddData = data.select(featureName).rdd.map {
      case Row(feature: Double) => feature
    }

    val ksTestResult = kolmogorovSmirnovTest(rddData, distName, distParams : _*)

    new KSTestWrapper(ksTestResult, distName, distParams)
  }
}

