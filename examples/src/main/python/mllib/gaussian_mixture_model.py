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
A Gaussian Mixture Model clustering program using MLlib.
"""
from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

import random
import argparse
import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import GaussianMixture


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


if __name__ == "__main__":
    """
    Parameters
    ----------
    :param inputFile:        Input file path which contains data points
    :param k:                Number of mixture components
    :param convergenceTol:   Convergence threshold. Default to 1e-3
    :param maxIterations:    Number of EM iterations to perform. Default to 100
    :param seed:             Random seed
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', help='Input File')
    parser.add_argument('k', type=int, help='Number of clusters')
    parser.add_argument('--convergenceTol', default=1e-3, type=float, help='convergence threshold')
    parser.add_argument('--maxIterations', default=100, type=int, help='Number of iterations')
    parser.add_argument('--seed', default=random.getrandbits(19),
                        type=long, help='Random seed')
    args = parser.parse_args()

    conf = SparkConf().setAppName("GMM")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(args.inputFile)
    data = lines.map(parseVector)
    model = GaussianMixture.train(data, args.k, args.convergenceTol,
                                  args.maxIterations, args.seed)
    for i in range(args.k):
        print(("weight = ", model.weights[i], "mu = ", model.gaussians[i].mu,
               "sigma = ", model.gaussians[i].sigma.toArray()))
    print("\n")
    print(("The membership value of each vector to all mixture components (first 100): ",
           model.predictSoft(data).take(100)))
    print("\n")
    print(("Cluster labels (first 100): ", model.predict(data).take(100)))
    sc.stop()
