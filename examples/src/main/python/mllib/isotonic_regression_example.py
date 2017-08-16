#
# © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
#
# This software is a modification of the original software Apache Spark licensed under the Apache 2.0
# license, a copy of which is below. This software contains proprietary information of
# Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
# otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
# without express written authorization from Stratio Big Data Inc., Sucursal en España.
#

"""
Isotonic Regression Example.
"""
from __future__ import print_function

from pyspark import SparkContext
# $example on$
import math
from pyspark.mllib.regression import LabeledPoint, IsotonicRegression, IsotonicRegressionModel
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="PythonIsotonicRegressionExample")

    # $example on$
    # Load and parse the data
    def parsePoint(labeledData):
        return (labeledData.label, labeledData.features[0], 1.0)

    data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_isotonic_regression_libsvm_data.txt")

    # Create label, feature, weight tuples from input data with weight set to default value 1.0.
    parsedData = data.map(parsePoint)

    # Split data into training (60%) and test (40%) sets.
    training, test = parsedData.randomSplit([0.6, 0.4], 11)

    # Create isotonic regression model from training data.
    # Isotonic parameter defaults to true so it is only shown for demonstration
    model = IsotonicRegression.train(training)

    # Create tuples of predicted and real labels.
    predictionAndLabel = test.map(lambda p: (model.predict(p[1]), p[0]))

    # Calculate mean squared error between predicted and real labels.
    meanSquaredError = predictionAndLabel.map(lambda pl: math.pow((pl[0] - pl[1]), 2)).mean()
    print("Mean Squared Error = " + str(meanSquaredError))

    # Save and load model
    model.save(sc, "target/tmp/myIsotonicRegressionModel")
    sameModel = IsotonicRegressionModel.load(sc, "target/tmp/myIsotonicRegressionModel")
    # $example off$
