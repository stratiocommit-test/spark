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
from pyspark.mllib.feature import Word2Vec
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="Word2VecExample")  # SparkContext

    # $example on$
    inp = sc.textFile("data/mllib/sample_lda_data.txt").map(lambda row: row.split(" "))

    word2vec = Word2Vec()
    model = word2vec.fit(inp)

    synonyms = model.findSynonyms('1', 5)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))
    # $example off$

    sc.stop()
