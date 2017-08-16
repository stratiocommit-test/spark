/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.annotation.Since

/**
 * Represents a distributively stored matrix backed by one or more RDDs.
 */
@Since("1.0.0")
trait DistributedMatrix extends Serializable {

  /** Gets or computes the number of rows. */
  @Since("1.0.0")
  def numRows(): Long

  /** Gets or computes the number of columns. */
  @Since("1.0.0")
  def numCols(): Long

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double]
}
