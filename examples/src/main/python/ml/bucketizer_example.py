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

from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.feature import Bucketizer
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BucketizerExample")\
        .getOrCreate()

    # $example on$
    splits = [-float("inf"), -0.5, 0.0, 0.5, float("inf")]

    data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
    dataFrame = spark.createDataFrame(data, ["features"])

    bucketizer = Bucketizer(splits=splits, inputCol="features", outputCol="bucketedFeatures")

    # Transform original data into its bucket index.
    bucketedData = bucketizer.transform(dataFrame)

    print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits())-1))
    bucketedData.show()
    # $example off$

    spark.stop()
