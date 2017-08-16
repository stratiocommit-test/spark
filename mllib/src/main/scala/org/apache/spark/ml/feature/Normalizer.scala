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
import org.apache.spark.ml.param.{DoubleParam, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.types.DataType

/**
 * Normalize a vector to have unit norm using the given p-norm.
 */
@Since("1.4.0")
class Normalizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[Vector, Vector, Normalizer] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("normalizer"))

  /**
   * Normalization in L^p^ space. Must be greater than equal to 1.
   * (default: p = 2)
   * @group param
   */
  @Since("1.4.0")
  val p = new DoubleParam(this, "p", "the p norm value", ParamValidators.gtEq(1))

  setDefault(p -> 2.0)

  /** @group getParam */
  @Since("1.4.0")
  def getP: Double = $(p)

  /** @group setParam */
  @Since("1.4.0")
  def setP(value: Double): this.type = set(p, value)

  override protected def createTransformFunc: Vector => Vector = {
    val normalizer = new feature.Normalizer($(p))
    vector => normalizer.transform(OldVectors.fromML(vector)).asML
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

@Since("1.6.0")
object Normalizer extends DefaultParamsReadable[Normalizer] {

  @Since("1.6.0")
  override def load(path: String): Normalizer = super.load(path)
}
