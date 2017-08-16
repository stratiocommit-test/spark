/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.param

import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasMaxIter}
import org.apache.spark.ml.util.Identifiable

/** A subclass of Params for testing. */
class TestParams(override val uid: String) extends Params with HasHandleInvalid with HasMaxIter
    with HasInputCol {

  def this() = this(Identifiable.randomUID("testParams"))

  def setMaxIter(value: Int): this.type = { set(maxIter, value); this }

  def setInputCol(value: String): this.type = { set(inputCol, value); this }

  setDefault(maxIter -> 10)

  def clearMaxIter(): this.type = clear(maxIter)

  override def copy(extra: ParamMap): TestParams = defaultCopy(extra)
}
