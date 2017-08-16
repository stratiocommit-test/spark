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
from pyspark.mllib.feature import ElementwiseProduct
from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="ElementwiseProductExample")  # SparkContext

    # $example on$
    data = sc.textFile("data/mllib/kmeans_data.txt")
    parsedData = data.map(lambda x: [float(t) for t in x.split(" ")])

    # Create weight vector.
    transformingVector = Vectors.dense([0.0, 1.0, 2.0])
    transformer = ElementwiseProduct(transformingVector)

    # Batch transform
    transformedData = transformer.transform(parsedData)
    # Single-row transform
    transformedData2 = transformer.transform(parsedData.first())
    # $example off$

    print("transformedData:")
    for each in transformedData.collect():
        print(each)

    print("transformedData2:")
    for each in transformedData2.collect():
        print(each)

    sc.stop()
