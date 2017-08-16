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

# $example on$
from pyspark.ml.regression import IsotonicRegression
# $example off$
from pyspark.sql import SparkSession

"""
An example demonstrating isotonic regression.
Run with:
  bin/spark-submit examples/src/main/python/ml/isotonic_regression_example.py
"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("IsotonicRegressionExample")\
        .getOrCreate()

    # $example on$
    # Loads data.
    dataset = spark.read.format("libsvm")\
        .load("data/mllib/sample_isotonic_regression_libsvm_data.txt")

    # Trains an isotonic regression model.
    model = IsotonicRegression().fit(dataset)
    print("Boundaries in increasing order: %s\n" % str(model.boundaries))
    print("Predictions associated with the boundaries: %s\n" % str(model.predictions))

    # Makes predictions.
    model.transform(dataset).show()
    # $example off$

    spark.stop()
