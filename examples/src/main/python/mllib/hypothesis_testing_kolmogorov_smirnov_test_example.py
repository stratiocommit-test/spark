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
from pyspark.mllib.stat import Statistics
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="HypothesisTestingKolmogorovSmirnovTestExample")

    # $example on$
    parallelData = sc.parallelize([0.1, 0.15, 0.2, 0.3, 0.25])

    # run a KS test for the sample versus a standard normal distribution
    testResult = Statistics.kolmogorovSmirnovTest(parallelData, "norm", 0, 1)
    # summary of the test including the p-value, test statistic, and null hypothesis
    # if our p-value indicates significance, we can reject the null hypothesis
    # Note that the Scala functionality of calling Statistics.kolmogorovSmirnovTest with
    # a lambda to calculate the CDF is not made available in the Python API
    print(testResult)
    # $example off$

    sc.stop()
