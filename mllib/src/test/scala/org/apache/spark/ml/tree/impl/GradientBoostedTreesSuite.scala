/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.tree.impl

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTreesSuite => OldGBTSuite}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.tree.loss.{AbsoluteError, LogLoss, SquaredError}
import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
 * Test suite for [[GradientBoostedTrees]].
 */
class GradientBoostedTreesSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {

  import testImplicits._

  test("runWithValidation stops early and performs better on a validation dataset") {
    // Set numIterations large enough so that it stops early.
    val numIterations = 20
    val trainRdd = sc.parallelize(OldGBTSuite.trainData, 2).map(_.asML)
    val validateRdd = sc.parallelize(OldGBTSuite.validateData, 2).map(_.asML)
    val trainDF = trainRdd.toDF()
    val validateDF = validateRdd.toDF()

    val algos = Array(Regression, Regression, Classification)
    val losses = Array(SquaredError, AbsoluteError, LogLoss)
    algos.zip(losses).foreach { case (algo, loss) =>
      val treeStrategy = new Strategy(algo = algo, impurity = Variance, maxDepth = 2,
        categoricalFeaturesInfo = Map.empty)
      val boostingStrategy =
        new BoostingStrategy(treeStrategy, loss, numIterations, validationTol = 0.0)
      val (validateTrees, validateTreeWeights) = GradientBoostedTrees
        .runWithValidation(trainRdd, validateRdd, boostingStrategy, 42L)
      val numTrees = validateTrees.length
      assert(numTrees !== numIterations)

      // Test that it performs better on the validation dataset.
      val (trees, treeWeights) = GradientBoostedTrees.run(trainRdd, boostingStrategy, 42L)
      val (errorWithoutValidation, errorWithValidation) = {
        if (algo == Classification) {
          val remappedRdd = validateRdd.map(x => new LabeledPoint(2 * x.label - 1, x.features))
          (GradientBoostedTrees.computeError(remappedRdd, trees, treeWeights, loss),
            GradientBoostedTrees.computeError(remappedRdd, validateTrees,
              validateTreeWeights, loss))
        } else {
          (GradientBoostedTrees.computeError(validateRdd, trees, treeWeights, loss),
            GradientBoostedTrees.computeError(validateRdd, validateTrees,
              validateTreeWeights, loss))
        }
      }
      assert(errorWithValidation <= errorWithoutValidation)

      // Test that results from evaluateEachIteration comply with runWithValidation.
      // Note that convergenceTol is set to 0.0
      val evaluationArray = GradientBoostedTrees
        .evaluateEachIteration(validateRdd, trees, treeWeights, loss, algo)
      assert(evaluationArray.length === numIterations)
      assert(evaluationArray(numTrees) > evaluationArray(numTrees - 1))
      var i = 1
      while (i < numTrees) {
        assert(evaluationArray(i) <= evaluationArray(i - 1))
        i += 1
      }
    }
  }

}
