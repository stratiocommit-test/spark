#
# © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
#
# This software is a modification of the original software Apache Spark licensed under the Apache 2.0
# license, a copy of which is below. This software contains proprietary information of
# Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
# otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
# without express written authorization from Stratio Big Data Inc., Sucursal en España.
#

from __future__ import print_function

# $example on$
from pyspark.ml.clustering import BisectingKMeans
# $example off$
from pyspark.sql import SparkSession

"""
An example demonstrating bisecting k-means clustering.
Run with:
  bin/spark-submit examples/src/main/python/ml/bisecting_k_means_example.py
"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BisectingKMeansExample")\
        .getOrCreate()

    # $example on$
    # Loads data.
    dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    # Trains a bisecting k-means model.
    bkm = BisectingKMeans().setK(2).setSeed(1)
    model = bkm.fit(dataset)

    # Evaluate clustering.
    cost = model.computeCost(dataset)
    print("Within Set Sum of Squared Errors = " + str(cost))

    # Shows the result.
    print("Cluster Centers: ")
    centers = model.clusterCenters()
    for center in centers:
        print(center)
    # $example off$

    spark.stop()
