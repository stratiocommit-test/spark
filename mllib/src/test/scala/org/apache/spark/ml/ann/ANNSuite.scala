/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.ann

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class ANNSuite extends SparkFunSuite with MLlibTestSparkContext {

  // TODO: test for weights comparison with Weka MLP
  test("ANN with Sigmoid learns XOR function with LBFGS optimizer") {
    val inputs = Array(
      Array(0.0, 0.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0),
      Array(1.0, 1.0)
    )
    val outputs = Array(0.0, 1.0, 1.0, 0.0)
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(label))
    }
    val rddData = sc.parallelize(data, 1)
    val hiddenLayersTopology = Array(5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights
    val trainer = new FeedForwardTrainer(topology, 2, 1)
    trainer.setWeights(initialWeights)
    trainer.LBFGSOptimizer.setNumIterations(20)
    val model = trainer.train(rddData)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input)(0), label(0))
    }.collect()
    predictionAndLabels.foreach { case (p, l) =>
      assert(math.round(p) === l)
    }
  }

  test("ANN with SoftMax learns XOR function with 2-bit output and batch GD optimizer") {
    val inputs = Array(
      Array(0.0, 0.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0),
      Array(1.0, 1.0)
    )
    val outputs = Array(
      Array(1.0, 0.0),
      Array(0.0, 1.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0)
    )
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(label))
    }
    val rddData = sc.parallelize(data, 1)
    val hiddenLayersTopology = Array(5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights
    val trainer = new FeedForwardTrainer(topology, 2, 2)
    // TODO: add a test for SGD
    trainer.LBFGSOptimizer.setConvergenceTol(1e-4).setNumIterations(20)
    trainer.setWeights(initialWeights).setStackSize(1)
    val model = trainer.train(rddData)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input), label)
    }.collect()
    predictionAndLabels.foreach { case (p, l) =>
      assert(p ~== l absTol 0.5)
    }
  }
}
