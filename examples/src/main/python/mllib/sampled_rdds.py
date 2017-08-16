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
Randomly sampled RDDs.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.util import MLUtils


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print("Usage: sampled_rdds <libsvm data file>", file=sys.stderr)
        exit(-1)
    if len(sys.argv) == 2:
        datapath = sys.argv[1]
    else:
        datapath = 'data/mllib/sample_binary_classification_data.txt'

    sc = SparkContext(appName="PythonSampledRDDs")

    fraction = 0.1  # fraction of data to sample

    examples = MLUtils.loadLibSVMFile(sc, datapath)
    numExamples = examples.count()
    if numExamples == 0:
        print("Error: Data file had no samples to load.", file=sys.stderr)
        exit(1)
    print('Loaded data with %d examples from file: %s' % (numExamples, datapath))

    # Example: RDD.sample() and RDD.takeSample()
    expectedSampleSize = int(numExamples * fraction)
    print('Sampling RDD using fraction %g.  Expected sample size = %d.'
          % (fraction, expectedSampleSize))
    sampledRDD = examples.sample(withReplacement=True, fraction=fraction)
    print('  RDD.sample(): sample has %d examples' % sampledRDD.count())
    sampledArray = examples.takeSample(withReplacement=True, num=expectedSampleSize)
    print('  RDD.takeSample(): sample has %d examples' % len(sampledArray))

    print()

    # Example: RDD.sampleByKey()
    keyedRDD = examples.map(lambda lp: (int(lp.label), lp.features))
    print('  Keyed data using label (Int) as key ==> Orig')
    #  Count examples per label in original data.
    keyCountsA = keyedRDD.countByKey()

    #  Subsample, and count examples per label in sampled data.
    fractions = {}
    for k in keyCountsA.keys():
        fractions[k] = fraction
    sampledByKeyRDD = keyedRDD.sampleByKey(withReplacement=True, fractions=fractions)
    keyCountsB = sampledByKeyRDD.countByKey()
    sizeB = sum(keyCountsB.values())
    print('  Sampled %d examples using approximate stratified sampling (by label). ==> Sample'
          % sizeB)

    #  Compare samples
    print('   \tFractions of examples with key')
    print('Key\tOrig\tSample')
    for k in sorted(keyCountsA.keys()):
        fracA = keyCountsA[k] / float(numExamples)
        if sizeB != 0:
            fracB = keyCountsB.get(k, 0) / float(sizeB)
        else:
            fracB = 0
        print('%d\t%g\t%g' % (k, fracA, fracB))

    sc.stop()
