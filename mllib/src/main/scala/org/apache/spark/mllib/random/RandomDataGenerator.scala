/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.random

import org.apache.commons.math3.distribution._

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.util.random.{Pseudorandom, XORShiftRandom}

/**
 * :: DeveloperApi ::
 * Trait for random data generators that generate i.i.d. data.
 */
@DeveloperApi
@Since("1.1.0")
trait RandomDataGenerator[T] extends Pseudorandom with Serializable {

  /**
   * Returns an i.i.d. sample as a generic type from an underlying distribution.
   */
  @Since("1.1.0")
  def nextValue(): T

  /**
   * Returns a copy of the RandomDataGenerator with a new instance of the rng object used in the
   * class when applicable for non-locking concurrent usage.
   */
  @Since("1.1.0")
  def copy(): RandomDataGenerator[T]
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from U[0.0, 1.0]
 */
@DeveloperApi
@Since("1.1.0")
class UniformGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  @Since("1.1.0")
  override def nextValue(): Double = {
    random.nextDouble()
  }

  @Since("1.1.0")
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  @Since("1.1.0")
  override def copy(): UniformGenerator = new UniformGenerator()
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the standard normal distribution.
 */
@DeveloperApi
@Since("1.1.0")
class StandardNormalGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  @Since("1.1.0")
  override def nextValue(): Double = {
      random.nextGaussian()
  }

  @Since("1.1.0")
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  @Since("1.1.0")
  override def copy(): StandardNormalGenerator = new StandardNormalGenerator()
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the Poisson distribution with the given mean.
 *
 * @param mean mean for the Poisson distribution.
 */
@DeveloperApi
@Since("1.1.0")
class PoissonGenerator @Since("1.1.0") (
    @Since("1.1.0") val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new PoissonDistribution(mean)

  @Since("1.1.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.1.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.1.0")
  override def copy(): PoissonGenerator = new PoissonGenerator(mean)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the exponential distribution with the given mean.
 *
 * @param mean mean for the exponential distribution.
 */
@DeveloperApi
@Since("1.3.0")
class ExponentialGenerator @Since("1.3.0") (
    @Since("1.3.0") val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new ExponentialDistribution(mean)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): ExponentialGenerator = new ExponentialGenerator(mean)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the gamma distribution with the given shape and scale.
 *
 * @param shape shape for the gamma distribution.
 * @param scale scale for the gamma distribution
 */
@DeveloperApi
@Since("1.3.0")
class GammaGenerator @Since("1.3.0") (
    @Since("1.3.0") val shape: Double,
    @Since("1.3.0") val scale: Double) extends RandomDataGenerator[Double] {

  private val rng = new GammaDistribution(shape, scale)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): GammaGenerator = new GammaGenerator(shape, scale)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the log normal distribution with the
 * given mean and standard deviation.
 *
 * @param mean mean for the log normal distribution.
 * @param std standard deviation for the log normal distribution
 */
@DeveloperApi
@Since("1.3.0")
class LogNormalGenerator @Since("1.3.0") (
    @Since("1.3.0") val mean: Double,
    @Since("1.3.0") val std: Double) extends RandomDataGenerator[Double] {

  private val rng = new LogNormalDistribution(mean, std)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): LogNormalGenerator = new LogNormalGenerator(mean, std)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the Weibull distribution with the
 * given shape and scale parameter.
 *
 * @param alpha shape parameter for the Weibull distribution.
 * @param beta scale parameter for the Weibull distribution.
 */
@DeveloperApi
class WeibullGenerator(
    val alpha: Double,
    val beta: Double) extends RandomDataGenerator[Double] {

  private val rng = new WeibullDistribution(alpha, beta)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): WeibullGenerator = new WeibullGenerator(alpha, beta)
}
