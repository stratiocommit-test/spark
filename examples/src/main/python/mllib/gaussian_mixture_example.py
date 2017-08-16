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
from pyspark.mllib.clustering import GaussianMixture, GaussianMixtureModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="GaussianMixtureExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/gmm_data.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.strip().split(' ')]))

    # Build the model (cluster the data)
    gmm = GaussianMixture.train(parsedData, 2)

    # Save and load model
    gmm.save(sc, "target/org/apache/spark/PythonGaussianMixtureExample/GaussianMixtureModel")
    sameModel = GaussianMixtureModel\
        .load(sc, "target/org/apache/spark/PythonGaussianMixtureExample/GaussianMixtureModel")

    # output parameters of model
    for i in range(2):
        print("weight = ", gmm.weights[i], "mu = ", gmm.gaussians[i].mu,
              "sigma = ", gmm.gaussians[i].sigma.toArray())
    # $example off$

    sc.stop()
