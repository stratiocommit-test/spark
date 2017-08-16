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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PowerIterationClusteringExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/pic_data.txt")
    similarities = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))

    # Cluster the data into two classes using PowerIterationClustering
    model = PowerIterationClustering.train(similarities, 2, 10)

    model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

    # Save and load model
    model.save(sc, "target/org/apache/spark/PythonPowerIterationClusteringExample/PICModel")
    sameModel = PowerIterationClusteringModel\
        .load(sc, "target/org/apache/spark/PythonPowerIterationClusteringExample/PICModel")
    # $example off$

    sc.stop()
