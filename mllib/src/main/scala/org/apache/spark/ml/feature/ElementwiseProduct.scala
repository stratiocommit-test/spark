/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.types.DataType

/**
 * Outputs the Hadamard product (i.e., the element-wise product) of each input vector with a
 * provided "weight" vector.  In other words, it scales each column of the dataset by a scalar
 * multiplier.
 */
@Since("1.4.0")
class ElementwiseProduct @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[Vector, Vector, ElementwiseProduct] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("elemProd"))

  /**
   * the vector to multiply with input vectors
   * @group param
   */
  @Since("2.0.0")
  val scalingVec: Param[Vector] = new Param(this, "scalingVec", "vector for hadamard product")

  /** @group setParam */
  @Since("2.0.0")
  def setScalingVec(value: Vector): this.type = set(scalingVec, value)

  /** @group getParam */
  @Since("2.0.0")
  def getScalingVec: Vector = getOrDefault(scalingVec)

  override protected def createTransformFunc: Vector => Vector = {
    require(params.contains(scalingVec), s"transformation requires a weight vector")
    val elemScaler = new feature.ElementwiseProduct($(scalingVec))
    v => elemScaler.transform(v)
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

@Since("2.0.0")
object ElementwiseProduct extends DefaultParamsReadable[ElementwiseProduct] {

  @Since("2.0.0")
  override def load(path: String): ElementwiseProduct = super.load(path)
}
