/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class AreaUnderCurveSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("auc computation") {
    val curve = Seq((0.0, 0.0), (1.0, 1.0), (2.0, 3.0), (3.0, 0.0))
    val auc = 4.0
    assert(AreaUnderCurve.of(curve) ~== auc absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== auc absTol 1E-5)
  }

  test("auc of an empty curve") {
    val curve = Seq.empty[(Double, Double)]
    assert(AreaUnderCurve.of(curve) ~== 0.0 absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== 0.0 absTol 1E-5)
  }

  test("auc of a curve with a single point") {
    val curve = Seq((1.0, 1.0))
    assert(AreaUnderCurve.of(curve) ~== 0.0 absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== 0.0 absTol 1E-5)
  }
}
