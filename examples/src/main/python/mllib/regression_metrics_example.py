#
# © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
#
# This software is a modification of the original software Apache Spark licensed under the Apache 2.0
# license, a copy of which is below. This software contains proprietary information of
# Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
# otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
# without express written authorization from Stratio Big Data Inc., Sucursal en España.
#

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import DenseVector
# $example off$

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="Regression Metrics Example")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        values = line.split()
        return LabeledPoint(float(values[0]),
                            DenseVector([float(x.split(':')[1]) for x in values[1:]]))

    data = sc.textFile("data/mllib/sample_linear_regression_data.txt")
    parsedData = data.map(parsePoint)

    # Build the model
    model = LinearRegressionWithSGD.train(parsedData)

    # Get predictions
    valuesAndPreds = parsedData.map(lambda p: (float(model.predict(p.features)), p.label))

    # Instantiate metrics object
    metrics = RegressionMetrics(valuesAndPreds)

    # Squared Error
    print("MSE = %s" % metrics.meanSquaredError)
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    # R-squared
    print("R-squared = %s" % metrics.r2)

    # Mean absolute error
    print("MAE = %s" % metrics.meanAbsoluteError)

    # Explained variance
    print("Explained variance = %s" % metrics.explainedVariance)
    # $example off$
