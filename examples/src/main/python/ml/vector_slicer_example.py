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
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("VectorSlicerExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        Row(userFeatures=Vectors.sparse(3, {0: -2.0, 1: 2.3})),
        Row(userFeatures=Vectors.dense([-2.0, 2.3, 0.0]))])

    slicer = VectorSlicer(inputCol="userFeatures", outputCol="features", indices=[1])

    output = slicer.transform(df)

    output.select("userFeatures", "features").show()
    # $example off$

    spark.stop()
