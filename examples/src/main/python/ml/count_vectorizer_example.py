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
from pyspark.ml.feature import CountVectorizer
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("CountVectorizerExample")\
        .getOrCreate()

    # $example on$
    # Input data: Each row is a bag of words with a ID.
    df = spark.createDataFrame([
        (0, "a b c".split(" ")),
        (1, "a b b c a".split(" "))
    ], ["id", "words"])

    # fit a CountVectorizerModel from the corpus.
    cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)

    model = cv.fit(df)

    result = model.transform(df)
    result.show(truncate=False)
    # $example off$

    spark.stop()
