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
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
 * A feature transformer that converts the input array of strings into an array of n-grams. Null
 * values in the input array are ignored.
 * It returns an array of n-grams where each n-gram is represented by a space-separated string of
 * words.
 *
 * When the input is empty, an empty array is returned.
 * When the input array length is less than n (number of elements per n-gram), no n-grams are
 * returned.
 */
@Since("1.5.0")
class NGram @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends UnaryTransformer[Seq[String], Seq[String], NGram] with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("ngram"))

  /**
   * Minimum n-gram length, greater than or equal to 1.
   * Default: 2, bigram features
   * @group param
   */
  @Since("1.5.0")
  val n: IntParam = new IntParam(this, "n", "number elements per n-gram (>=1)",
    ParamValidators.gtEq(1))

  /** @group setParam */
  @Since("1.5.0")
  def setN(value: Int): this.type = set(n, value)

  /** @group getParam */
  @Since("1.5.0")
  def getN: Int = $(n)

  setDefault(n -> 2)

  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    _.iterator.sliding($(n)).withPartial(false).map(_.mkString(" ")).toSeq
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}

@Since("1.6.0")
object NGram extends DefaultParamsReadable[NGram] {

  @Since("1.6.0")
  override def load(path: String): NGram = super.load(path)
}
