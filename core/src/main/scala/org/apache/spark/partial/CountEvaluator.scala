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

import org.apache.commons.math3.distribution.{PascalDistribution, PoissonDistribution}

/**
 * An ApproximateEvaluator for counts.
 */
private[spark] class CountEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[Long, BoundedDouble] {

  private var outputsMerged = 0
  private var sum: Long = 0

  override def merge(outputId: Int, taskResult: Long): Unit = {
    outputsMerged += 1
    sum += taskResult
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(sum, 1.0, sum, sum)
    } else if (outputsMerged == 0 || sum == 0) {
      new BoundedDouble(0, 0.0, 0.0, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      CountEvaluator.bound(confidence, sum, p)
    }
  }
}

private[partial] object CountEvaluator {

  def bound(confidence: Double, sum: Long, p: Double): BoundedDouble = {
    // Let the total count be N. A fraction p has been counted already, with sum 'sum',
    // as if each element from the total data set had been seen with probability p.
    val dist =
      if (sum <= 10000) {
        // The remaining count, k=N-sum, may be modeled as negative binomial (aka Pascal),
        // where there have been 'sum' successes of probability p already. (There are several
        // conventions, but this is the one followed by Commons Math3.)
        new PascalDistribution(sum.toInt, p)
      } else {
        // For large 'sum' (certainly, > Int.MaxValue!), use a Poisson approximation, which has
        // a different interpretation. "sum" elements have been observed having scanned a fraction
        // p of the data. This suggests data is counted at a rate of sum / p across the whole data
        // set. The total expected count from the rest is distributed as
        // (1-p) Poisson(sum / p) = Poisson(sum*(1-p)/p)
        new PoissonDistribution(sum * (1 - p) / p)
      }
    // Not quite symmetric; calculate interval straight from discrete distribution
    val low = dist.inverseCumulativeProbability((1 - confidence) / 2)
    val high = dist.inverseCumulativeProbability((1 + confidence) / 2)
    // Add 'sum' to each because distribution is just of remaining count, not observed
    new BoundedDouble(sum + dist.getNumericalMean, confidence, sum + low, sum + high)
  }


}
