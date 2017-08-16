/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaBucketizerSuite extends SharedSparkSession {

  @Test
  public void bucketizerTest() {
    double[] splits = {-0.5, 0.0, 0.5};

    StructType schema = new StructType(new StructField[]{
      new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
    });
    Dataset<Row> dataset = spark.createDataFrame(
      Arrays.asList(
        RowFactory.create(-0.5),
        RowFactory.create(-0.3),
        RowFactory.create(0.0),
        RowFactory.create(0.2)),
      schema);

    Bucketizer bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits);

    List<Row> result = bucketizer.transform(dataset).select("result").collectAsList();

    for (Row r : result) {
      double index = r.getDouble(0);
      Assert.assertTrue((index >= 0) && (index <= 1));
    }
  }
}
