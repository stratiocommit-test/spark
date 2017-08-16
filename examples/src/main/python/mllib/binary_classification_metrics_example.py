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
Binary Classification Metrics Example.
"""
from __future__ import print_function
from pyspark.sql import SparkSession
# $example on$
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BinaryClassificationMetricsExample")\
        .getOrCreate()

    # $example on$
    # Several of the methods available in scala are currently missing from pyspark
    # Load training data in LIBSVM format
    data = spark\
        .read.format("libsvm").load("data/mllib/sample_binary_classification_data.txt")\
        .rdd.map(lambda row: LabeledPoint(row[0], row[1]))

    # Split data into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4], seed=11)
    training.cache()

    # Run training algorithm to build the model
    model = LogisticRegressionWithLBFGS.train(training)

    # Compute raw scores on the test set
    predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))

    # Instantiate metrics object
    metrics = BinaryClassificationMetrics(predictionAndLabels)

    # Area under precision-recall curve
    print("Area under PR = %s" % metrics.areaUnderPR)

    # Area under ROC curve
    print("Area under ROC = %s" % metrics.areaUnderROC)
    # $example off$

    spark.stop()
