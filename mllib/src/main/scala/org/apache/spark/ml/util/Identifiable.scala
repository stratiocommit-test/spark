/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.util

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 *
 * Trait for an object with an immutable unique ID that identifies itself and its derivatives.
 *
 * WARNING: There have not yet been final discussions on this API, so it may be broken in future
 *          releases.
 */
@DeveloperApi
trait Identifiable {

  /**
   * An immutable unique ID for the object and its derivatives.
   */
  val uid: String

  override def toString: String = uid
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object Identifiable {

  /**
   * Returns a random UID that concatenates the given prefix, "_", and 12 random hex chars.
   */
  def randomUID(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.takeRight(12)
  }
}
