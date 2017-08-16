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

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaDCTSuite extends SharedSparkSession {

  @Test
  public void javaCompatibilityTest() {
    double[] input = new double[]{1D, 2D, 3D, 4D};
    Dataset<Row> dataset = spark.createDataFrame(
      Arrays.asList(RowFactory.create(Vectors.dense(input))),
      new StructType(new StructField[]{
        new StructField("vec", (new VectorUDT()), false, Metadata.empty())
      }));

    double[] expectedResult = input.clone();
    (new DoubleDCT_1D(input.length)).forward(expectedResult, true);

    DCT dct = new DCT()
      .setInputCol("vec")
      .setOutputCol("resultVec");

    List<Row> result = dct.transform(dataset).select("resultVec").collectAsList();
    Vector resultVec = result.get(0).getAs("resultVec");

    Assert.assertArrayEquals(expectedResult, resultVec.toArray(), 1e-6);
  }
}
