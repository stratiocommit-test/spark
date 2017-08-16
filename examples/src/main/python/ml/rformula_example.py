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
from pyspark.ml.feature import RFormula
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("RFormulaExample")\
        .getOrCreate()

    # $example on$
    dataset = spark.createDataFrame(
        [(7, "US", 18, 1.0),
         (8, "CA", 12, 0.0),
         (9, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])

    formula = RFormula(
        formula="clicked ~ country + hour",
        featuresCol="features",
        labelCol="label")

    output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
    # $example off$

    spark.stop()
