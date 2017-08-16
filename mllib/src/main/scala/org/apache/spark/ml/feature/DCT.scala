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

import edu.emory.mathcs.jtransforms.dct._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.BooleanParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.DataType

/**
 * A feature transformer that takes the 1D discrete cosine transform of a real vector. No zero
 * padding is performed on the input vector.
 * It returns a real vector of the same length representing the DCT. The return vector is scaled
 * such that the transform matrix is unitary (aka scaled DCT-II).
 *
 * More information on <a href="https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II">
 * DCT-II in Discrete cosine transform (Wikipedia)</a>.
 */
@Since("1.5.0")
class DCT @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends UnaryTransformer[Vector, Vector, DCT] with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("dct"))

  /**
   * Indicates whether to perform the inverse DCT (true) or forward DCT (false).
   * Default: false
   * @group param
   */
  @Since("1.5.0")
  def inverse: BooleanParam = new BooleanParam(
    this, "inverse", "Set transformer to perform inverse DCT")

  /** @group setParam */
  @Since("1.5.0")
  def setInverse(value: Boolean): this.type = set(inverse, value)

  /** @group getParam */
  @Since("1.5.0")
  def getInverse: Boolean = $(inverse)

  setDefault(inverse -> false)

  override protected def createTransformFunc: Vector => Vector = { vec =>
    val result = vec.toArray
    val jTransformer = new DoubleDCT_1D(result.length)
    if ($(inverse)) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[VectorUDT], s"Input type must be VectorUDT but got $inputType.")
  }

  override protected def outputDataType: DataType = new VectorUDT
}

@Since("1.6.0")
object DCT extends DefaultParamsReadable[DCT] {

  @Since("1.6.0")
  override def load(path: String): DCT = super.load(path)
}
