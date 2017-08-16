/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.util

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * A collection of methods used to validate data before applying ML algorithms.
 */
@DeveloperApi
@Since("0.8.0")
object DataValidators extends Logging {

  /**
   * Function to check if labels used for classification are either zero or one.
   *
   * @return True if labels are all zero or one, false otherwise.
   */
  @Since("1.0.0")
  val binaryLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    val numInvalid = data.filter(x => x.label != 1.0 && x.label != 0.0).count()
    if (numInvalid != 0) {
      logError("Classification labels should be 0 or 1. Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }

  /**
   * Function to check if labels used for k class multi-label classification are
   * in the range of {0, 1, ..., k - 1}.
   *
   * @return True if labels are all in the range of {0, 1, ..., k-1}, false otherwise.
   */
  @Since("1.3.0")
  def multiLabelValidator(k: Int): RDD[LabeledPoint] => Boolean = { data =>
    val numInvalid = data.filter(x =>
      x.label - x.label.toInt != 0.0 || x.label < 0 || x.label > k - 1).count()
    if (numInvalid != 0) {
      logError("Classification labels should be in {0 to " + (k - 1) + "}. " +
        "Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }
}
