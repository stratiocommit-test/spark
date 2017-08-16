#
# © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
#
# This software is a modification of the original software Apache Spark licensed under the Apache 2.0
# license, a copy of which is below. This software contains proprietary information of
# Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
# otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
# without express written authorization from Stratio Big Data Inc., Sucursal en España.
#

# This example uses text8 file from http://mattmahoney.net/dc/text8.zip
# The file was downloaded, unzipped and split into multiple lines using
#
# wget http://mattmahoney.net/dc/text8.zip
# unzip text8.zip
# grep -o -E '\w+(\W+\w+){0,15}' text8 > text8_lines
# This was done so that the example can be run in local mode

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

USAGE = ("bin/spark-submit --driver-memory 4g "
         "examples/src/main/python/mllib/word2vec.py text8_lines")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit("Argument for file not provided")
    file_path = sys.argv[1]
    sc = SparkContext(appName='Word2Vec')
    inp = sc.textFile(file_path).map(lambda row: row.split(" "))

    word2vec = Word2Vec()
    model = word2vec.fit(inp)

    synonyms = model.findSynonyms('china', 40)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))
    sc.stop()
