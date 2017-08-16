/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.stat.test

import org.apache.spark.annotation.Since

/**
 * Trait for hypothesis test results.
 * @tparam DF Return type of `degreesOfFreedom`.
 */
@Since("1.1.0")
trait TestResult[DF] {

  /**
   * The probability of obtaining a test statistic result at least as extreme as the one that was
   * actually observed, assuming that the null hypothesis is true.
   */
  @Since("1.1.0")
  def pValue: Double

  /**
   * Returns the degree(s) of freedom of the hypothesis test.
   * Return type should be Number(e.g. Int, Double) or tuples of Numbers for toString compatibility.
   */
  @Since("1.1.0")
  def degreesOfFreedom: DF

  /**
   * Test statistic.
   */
  @Since("1.1.0")
  def statistic: Double

  /**
   * Null hypothesis of the test.
   */
  @Since("1.1.0")
  def nullHypothesis: String

  /**
   * String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis: $nullHypothesis."
    } else {
      s"No presumption against null hypothesis: $nullHypothesis."
    }

    s"degrees of freedom = ${degreesOfFreedom.toString} \n" +
    s"statistic = $statistic \n" +
    s"pValue = $pValue \n" + pValueExplain
  }
}

/**
 * Object containing the test results for the chi-squared hypothesis test.
 */
@Since("1.1.0")
class ChiSqTestResult private[stat] (override val pValue: Double,
    @Since("1.1.0") override val degreesOfFreedom: Int,
    @Since("1.1.0") override val statistic: Double,
    @Since("1.1.0") val method: String,
    @Since("1.1.0") override val nullHypothesis: String) extends TestResult[Int] {

  override def toString: String = {
    "Chi squared test summary:\n" +
      s"method: $method\n" +
      super.toString
  }
}

/**
 * Object containing the test results for the Kolmogorov-Smirnov test.
 */
@Since("1.5.0")
class KolmogorovSmirnovTestResult private[stat] (
    @Since("1.5.0") override val pValue: Double,
    @Since("1.5.0") override val statistic: Double,
    @Since("1.5.0") override val nullHypothesis: String) extends TestResult[Int] {

  @Since("1.5.0")
  override val degreesOfFreedom = 0

  override def toString: String = {
    "Kolmogorov-Smirnov test summary:\n" + super.toString
  }
}

/**
 * Object containing the test results for streaming testing.
 */
@Since("1.6.0")
private[stat] class StreamingTestResult @Since("1.6.0") (
    @Since("1.6.0") override val pValue: Double,
    @Since("1.6.0") override val degreesOfFreedom: Double,
    @Since("1.6.0") override val statistic: Double,
    @Since("1.6.0") val method: String,
    @Since("1.6.0") override val nullHypothesis: String)
  extends TestResult[Double] with Serializable {

  override def toString: String = {
    "Streaming test summary:\n" +
      s"method: $method\n" +
      super.toString
  }
}

