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
Correlations using MLlib.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print("Usage: correlations (<file>)", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonCorrelations")
    if len(sys.argv) == 2:
        filepath = sys.argv[1]
    else:
        filepath = 'data/mllib/sample_linear_regression_data.txt'
    corrType = 'pearson'

    points = MLUtils.loadLibSVMFile(sc, filepath)\
        .map(lambda lp: LabeledPoint(lp.label, lp.features.toArray()))

    print()
    print('Summary of data file: ' + filepath)
    print('%d data points' % points.count())

    # Statistics (correlations)
    print()
    print('Correlation (%s) between label and each feature' % corrType)
    print('Feature\tCorrelation')
    numFeatures = points.take(1)[0].features.size
    labelRDD = points.map(lambda lp: lp.label)
    for i in range(numFeatures):
        featureRDD = points.map(lambda lp: lp.features[i])
        corr = Statistics.corr(labelRDD, featureRDD, corrType)
        print('%d\t%g' % (i, corr))
    print()

    sc.stop()
