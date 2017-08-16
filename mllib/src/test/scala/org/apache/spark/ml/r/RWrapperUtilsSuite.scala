/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.r

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{RFormula, RFormulaModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RWrapperUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("avoid libsvm data column name conflicting") {
    val rFormula = new RFormula().setFormula("label ~ features")
    val data = spark.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")

    // if not checking column name, then IllegalArgumentException
    intercept[IllegalArgumentException] {
      rFormula.fit(data)
    }

    // after checking, model build is ok
    RWrapperUtils.checkDataColumns(rFormula, data)

    assert(rFormula.getLabelCol == "label")
    assert(rFormula.getFeaturesCol.startsWith("features_"))

    val model = rFormula.fit(data)
    assert(model.isInstanceOf[RFormulaModel])

    assert(model.getLabelCol == "label")
    assert(model.getFeaturesCol.startsWith("features_"))
  }

}
