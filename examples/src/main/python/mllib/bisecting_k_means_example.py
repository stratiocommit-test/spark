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
from numpy import array
# $example off$

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonBisectingKMeansExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/kmeans_data.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

    # Build the model (cluster the data)
    model = BisectingKMeans.train(parsedData, 2, maxIterations=5)

    # Evaluate clustering
    cost = model.computeCost(parsedData)
    print("Bisecting K-means Cost = " + str(cost))

    # Save and load model
    path = "target/org/apache/spark/PythonBisectingKMeansExample/BisectingKMeansModel"
    model.save(sc, path)
    sameModel = BisectingKMeansModel.load(sc, path)
    # $example off$

    sc.stop()
