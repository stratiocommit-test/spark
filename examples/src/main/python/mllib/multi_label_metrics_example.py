#
# © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
#
# This software is a modification of the original software Apache Spark licensed under the Apache 2.0
# license, a copy of which is below. This software contains proprietary information of
# Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
# otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
# without express written authorization from Stratio Big Data Inc., Sucursal en España.
#

# $example on$
from pyspark.mllib.evaluation import MultilabelMetrics
# $example off$
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="MultiLabelMetricsExample")
    # $example on$
    scoreAndLabels = sc.parallelize([
        ([0.0, 1.0], [0.0, 2.0]),
        ([0.0, 2.0], [0.0, 1.0]),
        ([], [0.0]),
        ([2.0], [2.0]),
        ([2.0, 0.0], [2.0, 0.0]),
        ([0.0, 1.0, 2.0], [0.0, 1.0]),
        ([1.0], [1.0, 2.0])])

    # Instantiate metrics object
    metrics = MultilabelMetrics(scoreAndLabels)

    # Summary stats
    print("Recall = %s" % metrics.recall())
    print("Precision = %s" % metrics.precision())
    print("F1 measure = %s" % metrics.f1Measure())
    print("Accuracy = %s" % metrics.accuracy)

    # Individual label stats
    labels = scoreAndLabels.flatMap(lambda x: x[1]).distinct().collect()
    for label in labels:
        print("Class %s precision = %s" % (label, metrics.precision(label)))
        print("Class %s recall = %s" % (label, metrics.recall(label)))
        print("Class %s F1 Measure = %s" % (label, metrics.f1Measure(label)))

    # Micro stats
    print("Micro precision = %s" % metrics.microPrecision)
    print("Micro recall = %s" % metrics.microRecall)
    print("Micro F1 measure = %s" % metrics.microF1Measure)

    # Hamming loss
    print("Hamming loss = %s" % metrics.hammingLoss)

    # Subset accuracy
    print("Subset accuracy = %s" % metrics.subsetAccuracy)
    # $example off$
