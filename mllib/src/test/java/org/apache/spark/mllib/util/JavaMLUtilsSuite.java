/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.util;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaMLUtilsSuite extends SharedSparkSession {

  @Test
  public void testConvertVectorColumnsToAndFromML() {
    Vector x = Vectors.dense(2.0);
    Dataset<Row> dataset = spark.createDataFrame(
      Collections.singletonList(new LabeledPoint(1.0, x)), LabeledPoint.class
    ).select("label", "features");
    Dataset<Row> newDataset1 = MLUtils.convertVectorColumnsToML(dataset);
    Row new1 = newDataset1.first();
    Assert.assertEquals(RowFactory.create(1.0, x.asML()), new1);
    Row new2 = MLUtils.convertVectorColumnsToML(dataset, "features").first();
    Assert.assertEquals(new1, new2);
    Row old1 = MLUtils.convertVectorColumnsFromML(newDataset1).first();
    Assert.assertEquals(RowFactory.create(1.0, x), old1);
  }

  @Test
  public void testConvertMatrixColumnsToAndFromML() {
    Matrix x = Matrices.dense(2, 1, new double[]{1.0, 2.0});
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new MatrixUDT(), false, Metadata.empty())
    });
    Dataset<Row> dataset = spark.createDataFrame(
      Arrays.asList(
        RowFactory.create(1.0, x)),
      schema);

    Dataset<Row> newDataset1 = MLUtils.convertMatrixColumnsToML(dataset);
    Row new1 = newDataset1.first();
    Assert.assertEquals(RowFactory.create(1.0, x.asML()), new1);
    Row new2 = MLUtils.convertMatrixColumnsToML(dataset, "features").first();
    Assert.assertEquals(new1, new2);
    Row old1 = MLUtils.convertMatrixColumnsFromML(newDataset1).first();
    Assert.assertEquals(RowFactory.create(1.0, x), old1);
  }
}
