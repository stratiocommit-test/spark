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

if __name__ == "__main__":
    sc = SparkContext(appName="StratifiedSamplingExample")  # SparkContext

    # $example on$
    # an RDD of any key value pairs
    data = sc.parallelize([(1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')])

    # specify the exact fraction desired from each key as a dictionary
    fractions = {1: 0.1, 2: 0.6, 3: 0.3}

    approxSample = data.sampleByKey(False, fractions)
    # $example off$

    for each in approxSample.collect():
        print(each)

    sc.stop()
