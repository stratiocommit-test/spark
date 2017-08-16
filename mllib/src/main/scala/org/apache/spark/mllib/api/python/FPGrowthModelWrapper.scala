/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.api.python

import org.apache.spark.mllib.fpm.FPGrowthModel
import org.apache.spark.rdd.RDD

/**
 * A Wrapper of FPGrowthModel to provide helper method for Python
 */
private[python] class FPGrowthModelWrapper(model: FPGrowthModel[Any])
  extends FPGrowthModel(model.freqItemsets) {

  def getFreqItemsets: RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(model.freqItemsets.map(x => (x.javaItems, x.freq)))
  }
}
