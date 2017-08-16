/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.partial

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import org.apache.spark.util.StatCounter

/**
 * An ApproximateEvaluator for sums. It estimates the mean and the count and multiplies them
 * together, then uses the formula for the variance of two independent random variables to get
 * a variance for the result and compute a confidence interval.
 */
private[spark] class SumEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  // modified in merge
  private var outputsMerged = 0
  private val counter = new StatCounter()

  override def merge(outputId: Int, taskResult: StatCounter): Unit = {
    outputsMerged += 1
    counter.merge(taskResult)
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(counter.sum, 1.0, counter.sum, counter.sum)
    } else if (outputsMerged == 0 || counter.count == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      // Expected value of unobserved is presumed equal to that of the observed data
      val meanEstimate = counter.mean
      // Expected size of rest of the data is proportional
      val countEstimate = counter.count * (1 - p) / p
      // Expected sum is simply their product
      val sumEstimate = meanEstimate * countEstimate

      // Variance of unobserved data is presumed equal to that of the observed data
      val meanVar = counter.sampleVariance / counter.count

      // branch at this point because count == 1 implies counter.sampleVariance == Nan
      // and we don't want to ever return a bound of NaN
      if (meanVar.isNaN || counter.count == 1) {
        // add sum because estimate is of unobserved data sum
        new BoundedDouble(
          counter.sum + sumEstimate, confidence, Double.NegativeInfinity, Double.PositiveInfinity)
      } else {
        // See CountEvaluator. Variance of population count here follows from negative binomial
        val countVar = counter.count * (1 - p) / (p * p)
        // Var(Sum) = Var(Mean*Count) =
        // [E(Mean)]^2 * Var(Count) + [E(Count)]^2 * Var(Mean) + Var(Mean) * Var(Count)
        val sumVar = (meanEstimate * meanEstimate * countVar) +
          (countEstimate * countEstimate * meanVar) +
          (meanVar * countVar)
        val sumStdev = math.sqrt(sumVar)
        val confFactor = if (counter.count > 100) {
          new NormalDistribution().inverseCumulativeProbability((1 + confidence) / 2)
        } else {
          // note that if this goes to 0, TDistribution will throw an exception.
          // Hence special casing 1 above.
          val degreesOfFreedom = (counter.count - 1).toInt
          new TDistribution(degreesOfFreedom).inverseCumulativeProbability((1 + confidence) / 2)
        }
        // Symmetric, so confidence interval is symmetric about mean of distribution
        val low = sumEstimate - confFactor * sumStdev
        val high = sumEstimate + confFactor * sumStdev
        // add sum because estimate is of unobserved data sum
        new BoundedDouble(
          counter.sum + sumEstimate, confidence, counter.sum + low, counter.sum + high)
      }
    }
  }
}
