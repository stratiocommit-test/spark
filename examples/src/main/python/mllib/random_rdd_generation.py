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
Randomly generated RDDs.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.random import RandomRDDs


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print("Usage: random_rdd_generation", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonRandomRDDGeneration")

    numExamples = 10000  # number of examples to generate
    fraction = 0.1  # fraction of data to sample

    # Example: RandomRDDs.normalRDD
    normalRDD = RandomRDDs.normalRDD(sc, numExamples)
    print('Generated RDD of %d examples sampled from the standard normal distribution'
          % normalRDD.count())
    print('  First 5 samples:')
    for sample in normalRDD.take(5):
        print('    ' + str(sample))
    print()

    # Example: RandomRDDs.normalVectorRDD
    normalVectorRDD = RandomRDDs.normalVectorRDD(sc, numRows=numExamples, numCols=2)
    print('Generated RDD of %d examples of length-2 vectors.' % normalVectorRDD.count())
    print('  First 5 samples:')
    for sample in normalVectorRDD.take(5):
        print('    ' + str(sample))
    print()

    sc.stop()
