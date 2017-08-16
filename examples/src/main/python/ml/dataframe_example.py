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
An example of how to use DataFrame for ML. Run with::
    bin/spark-submit examples/src/main/python/ml/dataframe_example.py <input>
"""
from __future__ import print_function

import os
import sys
import tempfile
import shutil

from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: dataframe_example.py <libsvm file>", file=sys.stderr)
        exit(-1)
    elif len(sys.argv) == 2:
        input = sys.argv[1]
    else:
        input = "data/mllib/sample_libsvm_data.txt"

    spark = SparkSession \
        .builder \
        .appName("DataFrameExample") \
        .getOrCreate()

    # Load input data
    print("Loading LIBSVM file with UDT from " + input + ".")
    df = spark.read.format("libsvm").load(input).cache()
    print("Schema from LIBSVM:")
    df.printSchema()
    print("Loaded training data as a DataFrame with " +
          str(df.count()) + " records.")

    # Show statistical summary of labels.
    labelSummary = df.describe("label")
    labelSummary.show()

    # Convert features column to an RDD of vectors.
    features = MLUtils.convertVectorColumnsFromML(df, "features") \
        .select("features").rdd.map(lambda r: r.features)
    summary = Statistics.colStats(features)
    print("Selected features column with average values:\n" +
          str(summary.mean()))

    # Save the records in a parquet file.
    tempdir = tempfile.NamedTemporaryFile(delete=False).name
    os.unlink(tempdir)
    print("Saving to " + tempdir + " as Parquet file.")
    df.write.parquet(tempdir)

    # Load the records back.
    print("Loading Parquet file with UDT from " + tempdir)
    newDF = spark.read.parquet(tempdir)
    print("Schema from Parquet:")
    newDF.printSchema()
    shutil.rmtree(tempdir)

    spark.stop()
