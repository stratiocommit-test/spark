/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.feature

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for transformation of a vector
 */
@Since("1.1.0")
@DeveloperApi
trait VectorTransformer extends Serializable {

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  @Since("1.1.0")
  def transform(vector: Vector): Vector

  /**
   * Applies transformation on an RDD[Vector].
   *
   * @param data RDD[Vector] to be transformed.
   * @return transformed RDD[Vector].
   */
  @Since("1.1.0")
  def transform(data: RDD[Vector]): RDD[Vector] = {
    // Later in #1498 , all RDD objects are sent via broadcasting instead of RPC.
    // So it should be no longer necessary to explicitly broadcast `this` object.
    data.map(x => this.transform(x))
  }

  /**
   * Applies transformation on a JavaRDD[Vector].
   *
   * @param data JavaRDD[Vector] to be transformed.
   * @return transformed JavaRDD[Vector].
   */
  @Since("1.1.0")
  def transform(data: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(data.rdd)
  }

}
