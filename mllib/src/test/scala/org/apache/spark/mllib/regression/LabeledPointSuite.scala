/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{LabeledPoint => NewLabeledPoint}
import org.apache.spark.mllib.linalg.Vectors

class LabeledPointSuite extends SparkFunSuite {

  test("parse labeled points") {
    val points = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))
    points.foreach { p =>
      assert(p === LabeledPoint.parse(p.toString))
    }
  }

  test("parse labeled points with whitespaces") {
    val point = LabeledPoint.parse("(0.0, [1.0, 2.0])")
    assert(point === LabeledPoint(0.0, Vectors.dense(1.0, 2.0)))
  }

  test("parse labeled points with v0.9 format") {
    val point = LabeledPoint.parse("1.0,1.0 0.0 -2.0")
    assert(point === LabeledPoint(1.0, Vectors.dense(1.0, 0.0, -2.0)))
  }

  test("conversions between new ml LabeledPoint and mllib LabeledPoint") {
    val points: Seq[LabeledPoint] = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))

    val newPoints: Seq[NewLabeledPoint] = points.map(_.asML)

    points.zip(newPoints).foreach { case (p1, p2) =>
      assert(p1 === LabeledPoint.fromML(p2))
    }
  }
}
