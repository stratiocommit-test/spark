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
from pyspark.mllib.feature import StandardScaler, StandardScalerModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="StandardScalerExample")  # SparkContext

    # $example on$
    data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    label = data.map(lambda x: x.label)
    features = data.map(lambda x: x.features)

    scaler1 = StandardScaler().fit(features)
    scaler2 = StandardScaler(withMean=True, withStd=True).fit(features)

    # data1 will be unit variance.
    data1 = label.zip(scaler1.transform(features))

    # data2 will be unit variance and zero mean.
    data2 = label.zip(scaler2.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
    # $example off$

    print("data1:")
    for each in data1.collect():
        print(each)

    print("data2:")
    for each in data2.collect():
        print(each)

    sc.stop()
