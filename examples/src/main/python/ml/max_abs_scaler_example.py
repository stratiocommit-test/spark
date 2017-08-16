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
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MaxAbsScalerExample")\
        .getOrCreate()

    # $example on$
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -8.0]),),
        (1, Vectors.dense([2.0, 1.0, -4.0]),),
        (2, Vectors.dense([4.0, 10.0, 8.0]),)
    ], ["id", "features"])

    scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

    # Compute summary statistics and generate MaxAbsScalerModel
    scalerModel = scaler.fit(dataFrame)

    # rescale each feature to range [-1, 1].
    scaledData = scalerModel.transform(dataFrame)

    scaledData.select("features", "scaledFeatures").show()
    # $example off$

    spark.stop()
